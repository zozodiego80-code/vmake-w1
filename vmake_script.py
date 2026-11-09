#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Vmake subtitle remover — pipeline multi-comptes GitHub Actions.

Deux modes :

  local   Découpe la vidéo en morceaux de 4 s, les distribue en parallèle sur un
          pool de comptes GitHub (plusieurs amis), récupère les résultats, recolle
          la vidéo finale.

  action  Exécuté par GitHub Actions sur une machine neuve (IP neuve).
          Traite les morceaux assignés et dépose les résultats en artifact.

Transport :
  - aller  : les morceaux voyagent dans une Release GitHub (assets), jamais dans git
  - retour : les résultats voyagent en artifact GitHub Actions

Aucun `git push` de média, donc aucun conflit possible, quel que soit le nombre
de comptes utilisés en parallèle.
"""

import argparse
import base64
import io
import json
import os
import random
import shutil
import subprocess
import sys
import threading
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import requests
from tqdm import tqdm

# Console Windows : garantir l'UTF-8, sinon les emojis font planter le script.
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

# ============================================================
# CONFIGURATION
# ============================================================
BASE_URL         = "https://wapi.vmake.ai"
POLICY_URL       = "https://strategy.stariidata.com/upload/policy"
MAX_RETRIES      = 3
POLLING_INTERVAL = 3            # secondes entre deux vérifications Vmake

ROOT = Path(__file__).resolve().parent

DEFAULTS = {
    "chunk_duration":      4,      # secondes par morceau (marge sous le plafond de 5 s de Vmake)
    "chunks_per_job":      4,      # morceaux confiés à une machine GitHub
    "chunks_per_gid":      4,      # rotation du x-gid à l'intérieur d'un job
    "max_parallel_cap":    12,     # plafond de jobs simultanés
    "max_waves":           30,     # garde-fou anti-boucle
    "job_timeout":         2700,   # 45 min : temps max d'un job distant
    "run_lookup_timeout":  180,    # temps max pour voir apparaître le run après dispatch
    "artifact_retention":  1,      # jours
    "min_tail_duration":   2.0,    # fusionne le dernier morceau s'il est plus court
    "vmake_trim":          0.1,    # secondes rognees par Vmake au DEBUT de chaque morceau
                                   # (mesure : 3 images a 30 i/s). 0 = desactive.
}

FOLDERS = {
    "input":   ROOT / "input",
    "state":   ROOT / "state",
    "final":   ROOT / "final",
    "out":     ROOT / "out",
}


def log(msg: str = ""):
    print(msg, flush=True)


# ============================================================
# .env — chargement minimaliste (aucune dépendance supplémentaire)
# ============================================================
def load_dotenv(path: Path):
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, val = line.split("=", 1)
        key = key.strip()
        val = val.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = val


# ============================================================
# CONFIGURATION DES COMPTES
# ============================================================
class Account:
    """Un compte GitHub (Akram ou un ami) et ses dépôts-travailleurs."""

    def __init__(self, name: str, owner: str, workers: list, token: str,
                 parallel_cap: int | None = None):
        self.name         = name
        self.owner        = owner
        self.workers      = workers
        self.token        = token
        self.parallel_cap = parallel_cap

    def worker_full_names(self):
        return [f"{self.owner}/{w}" for w in self.workers]


def load_accounts(path: Path) -> dict:
    """Lit accounts.json et résout les tokens depuis l'environnement."""
    if not path.exists():
        sys.exit(
            f"❌ {path.name} introuvable.\n"
            f"   Copie {path.name.replace('.json', '.example.json')} vers {path.name} "
            f"et remplis tes comptes."
        )

    try:
        cfg = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        sys.exit(f"❌ {path.name} n'est pas un JSON valide : {e}")

    settings = {**DEFAULTS, **cfg.get("settings", {})}

    raw_accounts = cfg.get("accounts", [])
    if not raw_accounts:
        sys.exit("❌ accounts.json ne contient aucun compte.")

    accounts, seen = [], set()
    for i, raw in enumerate(raw_accounts):
        name    = raw.get("name") or f"compte{i + 1}"
        owner   = raw.get("owner", "").strip()
        workers = [w.strip() for w in raw.get("workers", []) if w.strip()]
        env_key = raw.get("token_env") or f"GITHUB_TOKEN_{name.upper().replace('-', '_')}"
        cap     = raw.get("parallel_cap")   # plafond propre à CE compte

        if not owner:
            sys.exit(f"❌ Compte « {name} » : le champ 'owner' est vide.")
        if not workers:
            sys.exit(f"❌ Compte « {name} » : aucun dépôt-travailleur listé.")

        # Ordre de résolution : variable dédiée, puis GITHUB_TOKEN générique.
        token = os.environ.get(env_key) or os.environ.get("GITHUB_TOKEN")
        if not token:
            sys.exit(
                f"❌ Compte « {name} » : token introuvable.\n"
                f"   Ajoute dans .env :  {env_key}=ton_token"
            )

        for w in workers:
            full = f"{owner}/{w}"
            if full in seen:
                sys.exit(f"❌ Dépôt en double dans accounts.json : {full}")
            seen.add(full)

        accounts.append(Account(name, owner, workers, token,
                                int(cap) if cap is not None else None))

    settings["_accounts"] = accounts
    return settings


# ============================================================
# FFMPEG
# ============================================================
def _require_ffmpeg():
    for tool in ("ffmpeg", "ffprobe"):
        if shutil.which(tool) is None:
            sys.exit(f"❌ {tool} est introuvable dans le PATH.")


def probe(path: Path) -> dict:
    out = subprocess.run(
        ["ffprobe", "-v", "error", "-print_format", "json",
         "-show_format", "-show_streams", str(path)],
        capture_output=True, text=True, check=True,
    ).stdout
    return json.loads(out)


def duration_of(path: Path) -> float:
    try:
        return float(probe(path)["format"]["duration"])
    except Exception:
        return 0.0


def _run_ffmpeg(cmd: list, what: str):
    r = subprocess.run(cmd, capture_output=True, text=True, errors="replace")
    if r.returncode != 0:
        tail = "\n".join((r.stderr or "").strip().splitlines()[-12:])
        raise RuntimeError(f"{what} a échoué (ffmpeg code {r.returncode}) :\n{tail}")


def _encode_chunk(input_path: Path, out_path: Path, start: float, length: float,
                  has_audio: bool = True):
    """Encode un morceau à position exacte, à partir de `start`, sur `length` secondes."""
    cmd = ["ffmpeg", "-y", "-hide_banner", "-loglevel", "error",
           "-ss", f"{start:.3f}", "-i", str(input_path),
           "-t", f"{length:.3f}",
           "-map", "0:v:0"]
    if has_audio:
        cmd += ["-map", "0:a?"]
    cmd += ["-c:v", "libx264", "-preset", "veryfast", "-crf", "18", "-pix_fmt", "yuv420p",
            "-c:a", "aac", "-b:a", "192k", "-ar", "44100", "-ac", "2",
            "-reset_timestamps", "1",
            str(out_path)]
    _run_ffmpeg(cmd, f"L'encodage du morceau {out_path.name}")


def split_video(input_path: Path, out_dir: Path, chunk_duration: int,
                cap: float = 5.0, preroll: float = 0.0) -> list:
    """
    Découpe en morceaux de durée exacte (correctif 1).

    Deux pièges corrigés ici :

    1. `-c copy` ne peut couper qu'aux images-clés existantes. Les morceaux
       sortaient entre 2,6 s et 7,8 s au lieu de 5 s → Vmake en tronquait 22 %.
       On réencode donc, ce qui permet de couper n'importe où.

    2. Le muxeur `-f segment` de ffmpeg, même avec `-force_key_frames`, ne coupe
       pas au premier point de coupe : mesuré à 8,02 s pour un premier morceau
       demandé à 4 s (il contenait pourtant bien une image-clé à 4 s, sans s'en
       servir). On abandonne `segment` pour une boucle `-ss`/`-t` explicite, qui
       donne exactement 4,00 s par morceau, de façon déterministe.

    Le plafond `cap` (5 s, imposé par Vmake) est vérifié après coup : mieux vaut
    un échec bruyant ici qu'une vidéo silencieusement tronquée à l'arrivée.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    for f in out_dir.glob("chunk_*.mp4"):
        f.unlink()

    total = duration_of(input_path)
    if total <= 0:
        raise RuntimeError(f"Durée illisible pour {input_path.name}")

    # Une video sans piste audio est valide : on ne tente pas de la retarder.
    has_audio = any(s.get("codec_type") == "audio" for s in probe(input_path)["streams"])

    n = max(1, int(total // chunk_duration) + (1 if total % chunk_duration else 0))
    for i in range(n):
        start  = i * chunk_duration
        length = min(chunk_duration, total - start)
        if length <= 0:
            break

        # `preroll` : Vmake rogne systematiquement les 3 PREMIERES images (0,1 s) de
        # chaque morceau qu'on lui envoie (mesure le 16/10/2026 : 3 images sur 120
        # pour CHACUN des 21 morceaux, 1,97 % de la video au total).
        # On lui donne donc `preroll` secondes d'avance qu'il consommera : la coupe
        # qu'il fait tombe exactement sur le debut voulu, et le contenu est preserve.
        #
        # Premier morceau : il n'existe rien avant t=0 a lui donner en avance. On le
        # laisse tel quel : Vmake en rognera les 0,1 s, soit 3 images perdues au tout
        # debut de la video — invisible, et de loin preferable aux deux alternatives
        # testees : geler la premiere image produit un micro-blocage de 0,1 s ET
        # decale le son (mesure le 16/10/2026).
        pre = preroll if i > 0 else 0.0
        src_start = max(0.0, start - pre)
        src_len   = length + (start - src_start)

        # Le plafond Vmake (5 s) ne doit jamais etre franchi : on retire l'avance
        # du morceau plutot que de risquer une troncature silencieuse.
        if src_len > cap:
            excess = src_len - cap
            src_start += excess
            src_len   -= excess

        _encode_chunk(input_path, out_dir / f"chunk_{i:03d}.mp4", src_start, src_len,
                      has_audio=has_audio)

    chunks = sorted(out_dir.glob("chunk_*.mp4"))

    # Garde-fou : aucun morceau ne doit dépasser le plafond de Vmake.
    oversized = []
    for c in chunks:
        d = duration_of(c)
        if d > cap:
            oversized.append((c.name, round(d, 2)))
    if oversized:
        raise RuntimeError(
            "Morceaux au-dessus du plafond Vmake de "
            f"{cap}s — le découpage a échoué : {oversized}"
        )

    return chunks


def _concat_list(parts: list, list_file: Path):
    """Écrit la liste de recollage (chemins normalisés en /, acceptés partout)."""
    with open(list_file, "w", encoding="utf-8") as f:
        for p in parts:
            safe = p.resolve().as_posix().replace("'", "'\\''")
            f.write(f"file '{safe}'\n")


def _concat_copy(parts: list, out_path: Path):
    """Recolle sans réencodage (parts issus du même encodeur)."""
    list_file = out_path.with_suffix(".txt")
    _concat_list(parts, list_file)
    try:
        _run_ffmpeg(
            ["ffmpeg", "-y", "-hide_banner", "-loglevel", "error",
             "-f", "concat", "-safe", "0", "-i", str(list_file),
             "-c", "copy", "-fflags", "+genpts", "-avoid_negative_ts", "make_zero",
             str(out_path)],
            "Le recollage",
        )
    finally:
        list_file.unlink(missing_ok=True)


def merge_short_tail(chunks: list, min_duration: float, cap: float = 5.0) -> list:
    """
    Fusionne le dernier morceau s'il est trop court, pour éviter un fragment inutile.

    La fusion n'a lieu que si le morceau obtenu reste SOUS le plafond de Vmake.
    Sans cette condition, fusionner un morceau de 4 s avec une queue de 1,02 s
    donne un morceau de 5,02 s que Vmake tronquerait en silence — soit exactement
    le bug que ce pipeline corrige. Une queue courte est inoffensive : on la garde.
    """
    if len(chunks) < 2:
        return chunks

    tail = duration_of(chunks[-1])
    if tail >= min_duration:
        return chunks

    prev = chunks[-2]
    combined = duration_of(prev) + tail
    # Marge de 0,25 s : la durée réellement encodée dépasse toujours un peu la
    # somme nominale (arrondi AAC, image ajoutée au recollage). Sans elle, un
    # 4,0 + 1,0 = 5,0 nominal ressort à 5,02 s et repasse au-dessus du plafond.
    if combined > cap - 0.25:
        log(f"   ℹ️  Dernier morceau de {tail:.2f}s conservé tel quel "
            f"(fusion à {combined:.2f}s refusée : trop proche du plafond de {cap}s)")
        return chunks

    merged = prev.with_name(prev.stem + "_merged.mp4")
    _concat_copy([prev, chunks[-1]], merged)
    chunks[-1].unlink()
    prev.unlink()
    merged.rename(prev)
    return sorted(chunks[0].parent.glob("chunk_*.mp4"))


def _homogeneous(parts: list) -> bool:
    """Vérifie que les morceaux partagent codec / résolution / cadence."""
    signatures = set()
    for p in parts:
        try:
            info = probe(p)
        except Exception:
            return False
        v = next((s for s in info["streams"] if s.get("codec_type") == "video"), None)
        if not v:
            return False
        signatures.add((v.get("codec_name"), v.get("width"), v.get("height"),
                        v.get("r_frame_rate")))
    return len(signatures) == 1


def concat_final(result_files: list, out_path: Path) -> Path:
    """
    Recolle les morceaux traités (correctif 8).

    Le `-c copy` d'origine était fragile. On l'essaie d'abord (aucune perte de
    qualité), on vérifie le résultat à la sonde, et on ne réencode qu'en cas
    d'incohérence avérée.
    """
    out_path.parent.mkdir(parents=True, exist_ok=True)
    expected = sum(duration_of(p) for p in result_files)

    if _homogeneous(result_files):
        _concat_copy(result_files, out_path)
        got = duration_of(out_path)
        if got > 0 and abs(got - expected) < max(1.0, expected * 0.02):
            return out_path
        log(f"   ⚠️  Recollage incohérent ({got:.2f}s pour {expected:.2f}s attendus) — réencodage")

    list_file = out_path.with_suffix(".txt")
    _concat_list(result_files, list_file)
    try:
        _run_ffmpeg(
            ["ffmpeg", "-y", "-hide_banner", "-loglevel", "error",
             "-f", "concat", "-safe", "0", "-i", str(list_file),
             "-c:v", "libx264", "-preset", "veryfast", "-crf", "18", "-pix_fmt", "yuv420p",
             "-c:a", "aac", "-b:a", "192k",
             str(out_path)],
            "Le recollage (réencodage)",
        )
    finally:
        list_file.unlink(missing_ok=True)
    return out_path


# ============================================================
# x-gid + headers Vmake
# ============================================================
def generate_xgid() -> str:
    def hx(n): return ''.join(random.choices('0123456789abcdef', k=n))
    def dg(n): return ''.join(random.choices('0123456789', k=n))
    return f"{hx(15)}-{hx(14)}-{hx(8)}-{dg(7)}-{hx(15)}"


def get_headers(xgid: str) -> dict:
    return {
        "accept":             "application/json, text/plain, */*",
        "accept-encoding":    "gzip, deflate, br, zstd",
        "accept-language":    "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
        "access-token":       "",
        "content-type":       "application/json",
        "origin":             "https://vmake.ai",
        "referer":            "https://vmake.ai/",
        "sec-ch-ua":          '"Chromium";v="154", "Google Chrome";v="154", "Not A(Brand";v="99"',
        "sec-ch-ua-mobile":   "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest":     "empty",
        "sec-fetch-mode":     "cors",
        "sec-fetch-site":     "same-site",
        "user-agent":         "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/154.0.0.0 Safari/537.36",
        "x-client-timezone": "GMT+02:00",
        "x-gid":              xgid,
        "cookie":             f"VMAKE_ANONYMOUS_GID={xgid}",
    }


class VmakeError(RuntimeError):
    def __init__(self, code, message):
        self.code = code
        super().__init__(f"code {code} — {message}")


# ============================================================
# APPELS VMAKE
# ============================================================
def get_sign(headers: dict) -> dict:
    url     = f"{BASE_URL}/uploader/sign.json"
    payload = {"count": 1, "suffix": "mp4", "tool_type": "watermark-remover",
               "source": "", "client_os": "Windows"}
    last = "inconnu"
    for attempt in range(MAX_RETRIES):
        try:
            data = requests.post(url, json=payload, headers=headers, timeout=30).json()
            if data["meta"]["code"] == 0:
                return data["response"]["params"]
            last = f"code {data['meta']['code']}"
        except Exception as e:
            last = f"{type(e).__name__}: {e}"
        time.sleep(5)
    raise VmakeError("sign", last)


def get_policy(params: dict) -> dict:
    query = {
        "app": params["app"], "count": 1,
        "sig": params["sig"], "sigTime": params["sigTime"],
        "sigVersion": params["sigVersion"], "suffix": "mp4",
        "type": params["type"],
    }
    last = "inconnu"
    for attempt in range(MAX_RETRIES):
        try:
            data = requests.get(POLICY_URL, params=query, timeout=30).json()
            if isinstance(data, list) and data:
                return data[0]["oss"]
            last = str(data)[:200]
        except Exception as e:
            last = f"{type(e).__name__}: {e}"
        time.sleep(5)
    raise VmakeError("policy", last)


def upload_to_oss(chunk_path: Path, oss_data: dict) -> str:
    try:
        import oss2
    except ImportError:
        sys.exit("❌ oss2 non installé — lance : pip install -r requirements.txt")

    cred   = oss_data["credentials"]
    auth   = oss2.StsAuth(cred["access_key"], cred["secret_key"], cred["session_token"])
    bucket = oss2.Bucket(auth, oss_data["url"], oss_data["bucket"])
    key    = oss_data["key"]
    last   = "inconnu"
    for attempt in range(MAX_RETRIES):
        try:
            with open(chunk_path, "rb") as f:
                bucket.put_object(key, f)
            return oss_data["data"]
        except Exception as e:
            last = f"{type(e).__name__}: {e}"
            time.sleep(5)
    raise VmakeError("oss", last)


def submit_task(url_video: str, filename: str, headers: dict) -> str:
    url     = f"{BASE_URL}/vm/tool/submit.json"
    hdr     = {**headers, "ab-list": "21801", "x-client-location": "32"}
    payload = {
        "url": url_video, "tool_type": "watermark-remover",
        "effect_model": "video_remove_full", "title": filename,
        "client_os": "Windows", "support_h_265": 0, "init_images": [],
    }
    last = "inconnu"
    for attempt in range(MAX_RETRIES):
        try:
            data = requests.post(url, json=payload, headers=hdr, timeout=30).json()
            code = data["meta"]["code"]
            if code == 0:
                return data["response"]["record_id"]
            # 10022 = quota anonyme épuisé pour ce couple (IP, x-gid).
            # Inutile d'insister : c'est au script local de replacer ce morceau
            # sur une autre machine.
            if code == 10022:
                raise VmakeError(code, "quota anonyme atteint")
            last = f"code {code}"
        except VmakeError:
            raise
        except Exception as e:
            last = f"{type(e).__name__}: {e}"
        time.sleep(5)
    raise VmakeError("submit", last)


def poll_task(record_id: str, headers: dict) -> str:
    url     = f"{BASE_URL}/vm/tool/query.json"
    payload = {"record_id": [record_id], "client_os": "Windows"}
    while True:
        try:
            data = requests.post(url, json=payload, headers=headers, timeout=30).json()
            if data["meta"]["code"] == 0:
                trial = data["response"]["list"][0]["trial_list"][0]
                if trial["status"] == 2:
                    return trial["download_url"]
                time.sleep(POLLING_INTERVAL)
            else:
                time.sleep(5)
        except Exception:
            time.sleep(5)


def download_result(url: str, out_path: Path) -> Path:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    last = "inconnu"
    for attempt in range(MAX_RETRIES):
        try:
            r = requests.get(url, stream=True, timeout=120)
            r.raise_for_status()
            with open(out_path, "wb") as f:
                for block in r.iter_content(65536):
                    f.write(block)
            return out_path
        except Exception as e:
            last = f"{type(e).__name__}: {e}"
            time.sleep(5)
    raise VmakeError("download", last)


def process_chunk(chunk_path: Path, idx: int, headers: dict, out_dir: Path) -> Path:
    sign_params = get_sign(headers)
    oss_data    = get_policy(sign_params)
    public_url  = upload_to_oss(chunk_path, oss_data)
    record_id   = submit_task(public_url, chunk_path.name, headers)
    dl_url      = poll_task(record_id, headers)
    return download_result(dl_url, out_dir / f"chunk_{idx:03d}.mp4")


# ============================================================
# CLIENT GITHUB
# ============================================================
class GitHubError(RuntimeError):
    pass


class GitHub:
    """Accès à l'API GitHub pour un dépôt donné, avec un token donné."""

    API = "https://api.github.com"

    def __init__(self, token: str, owner: str, repo: str):
        self.owner, self.repo = owner, repo
        self.label = f"{owner}/{repo}"
        self.s = requests.Session()
        self.s.headers.update({
            "Authorization":          f"Bearer {token}",
            "Accept":                 "application/vnd.github+json",
            "X-GitHub-Api-Version":   "2022-11-28",
            "User-Agent":             "vmake-pipeline/2.0",
        })

    # ---------- socle ----------
    def req(self, method: str, path: str, *, expect=(200, 201, 204), **kw):
        url  = path if path.startswith("http") else f"{self.API}{path}"
        last = "inconnu"
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                r = self.s.request(method, url, timeout=90, **kw)
            except requests.RequestException as e:
                last = f"{type(e).__name__}: {e}"
                time.sleep(2 * attempt)
                continue

            if r.status_code in expect:
                return r

            last = f"HTTP {r.status_code} — {r.text[:300]}"

            # Limite de débit : on attend et on réessaie.
            if r.status_code in (403, 429) and "rate limit" in r.text.lower():
                reset = r.headers.get("X-RateLimit-Reset")
                wait  = max(5, min(60, int(reset) - int(time.time()) + 2)) if reset else 30
                log(f"   ⏳ Limite GitHub atteinte sur {self.label} — pause {wait}s")
                time.sleep(wait)
                continue

            if r.status_code >= 500:
                time.sleep(2 * attempt)
                continue

            break

        raise GitHubError(f"{self.label} {method} {path} → {last}")

    # ---------- dépôt ----------
    def repo_exists(self) -> bool:
        return self.req("GET", f"/repos/{self.label}", expect=(200, 404)).status_code == 200

    def create_repo(self, private: bool = True, description: str = "") -> bool:
        r = self.req("POST", "/user/repos", expect=(201, 422), json={
            "name": self.repo, "private": private, "auto_init": True,
            "description": description or "Noeud de calcul Vmake",
            "has_issues": False, "has_wiki": False, "has_projects": False,
        })
        return r.status_code == 201

    def visibility(self) -> bool | None:
        """True si le dépôt est public, False s'il est privé, None s'il n'existe pas."""
        r = self.req("GET", f"/repos/{self.label}", expect=(200, 404))
        if r.status_code == 404:
            return None
        return not r.json()["private"]

    def set_visibility(self, public: bool):
        self.req("PATCH", f"/repos/{self.label}",
                 json={"private": not public}, expect=(200,))

    def put_file(self, path: str, data: bytes, message: str):
        """Crée ou met à jour un fichier du dépôt via l'API (aucun git local requis)."""
        r   = self.req("GET", f"/repos/{self.label}/contents/{path}", expect=(200, 404))
        sha = r.json()["sha"] if r.status_code == 200 else None
        body = {"message": message, "content": base64.b64encode(data).decode()}
        if sha:
            body["sha"] = sha
        return self.req("PUT", f"/repos/{self.label}/contents/{path}",
                        json=body, expect=(200, 201)).json()

    # ---------- releases (transport des morceaux) ----------
    def ensure_release(self, tag: str, name: str) -> dict:
        r = self.req("GET", f"/repos/{self.label}/releases/tags/{tag}", expect=(200, 404))
        if r.status_code == 200:
            return r.json()

        # 422 : deux jobs simultanes sur le MEME depot creent la release en meme
        # temps ; le second recoit un 422 trompeur (« must have a valid tag »)
        # alors que la release existe bel et bien. On la relit au lieu d'echouer.
        r = self.req("POST", f"/repos/{self.label}/releases", expect=(201, 422), json={
            "tag_name": tag, "name": name, "draft": False, "prerelease": False,
        })
        if r.status_code == 201:
            return r.json()

        r = self.req("GET", f"/repos/{self.label}/releases/tags/{tag}", expect=(200, 404))
        if r.status_code == 200:
            return r.json()
        raise GitHubError(f"{self.label} release « {tag} » introuvable après création")

    def upload_asset(self, release: dict, filename: str, data: bytes) -> dict:
        for a in release.get("assets", []):
            if a["name"] == filename:
                return a
        upload_url = release["upload_url"].split("{")[0]
        r = self.s.post(upload_url, params={"name": filename}, data=data,
                        headers={"Content-Type": "application/zip"}, timeout=600)
        if r.status_code not in (200, 201):
            raise GitHubError(f"{self.label} upload asset → HTTP {r.status_code} {r.text[:200]}")
        return r.json()

    def delete_release(self, tag: str):
        r = self.req("GET", f"/repos/{self.label}/releases/tags/{tag}", expect=(200, 404))
        if r.status_code != 200:
            return
        self.req("DELETE", f"/repos/{self.label}/releases/{r.json()['id']}", expect=(204,))

    # ---------- exécution du workflow ----------
    def dispatch(self, event_type: str, payload: dict):
        self.req("POST", f"/repos/{self.label}/dispatches", expect=(204,),
                 json={"event_type": event_type, "client_payload": payload})

    def find_run(self, run_name: str, timeout: int) -> dict | None:
        deadline = time.time() + timeout
        while time.time() < deadline:
            r = self.req("GET", f"/repos/{self.label}/actions/runs",
                         params={"event": "repository_dispatch", "per_page": 40})
            for run in r.json().get("workflow_runs", []):
                if run_name in (run.get("name"), run.get("display_title")):
                    return run
            time.sleep(4)
        return None

    def get_run(self, run_id: int) -> dict:
        return self.req("GET", f"/repos/{self.label}/actions/runs/{run_id}").json()

    def list_artifacts(self, run_id: int) -> list:
        return self.req("GET", f"/repos/{self.label}/actions/runs/{run_id}/artifacts").json()["artifacts"]

    def download_artifact(self, artifact_id: int) -> bytes:
        r = self.req("GET", f"/repos/{self.label}/actions/artifacts/{artifact_id}/zip",
                     stream=True)
        return r.content


# ============================================================
# ÉTAT (reprise après interruption)
# ============================================================
def video_id_for(input_path: Path) -> str:
    stamp = time.strftime("%Y%m%d-%H%M%S")
    safe  = "".join(c for c in input_path.stem if c.isalnum() or c in "-_")[:32]
    return f"{safe}-{stamp}"


def session_for(input_path: Path, reset: bool) -> str:
    """
    Retrouve la session d'une vidéo déjà commencée, ou en ouvre une neuve.

    Sans ça, chaque relance créait un nouvel identifiant, donc un nouveau dossier :
    la « reprise » annoncée repartait de zéro et refaisait tout le travail déjà
    payé. `--reset` force une session neuve.
    """
    safe = "".join(c for c in input_path.stem if c.isalnum() or c in "-_")[:32]
    if not reset:
        existantes = [d for d in FOLDERS["state"].glob(f"{safe}-*") if d.is_dir()]
        if existantes:
            # La plus récente : c'est la seule qui puisse contenir un travail utile.
            return max(existantes, key=lambda d: d.stat().st_mtime).name
    return video_id_for(input_path)


def save_state(state_dir: Path, name: str, data: dict):
    state_dir.mkdir(parents=True, exist_ok=True)
    (state_dir / name).write_text(json.dumps(data, indent=2), encoding="utf-8")


def load_state(state_dir: Path, name: str) -> dict | None:
    p = state_dir / name
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None


# ============================================================
# MODE ACTION — tourne sur la machine GitHub (IP neuve)
# ============================================================
def run_action(args) -> int:
    _require_ffmpeg()
    job_id    = args.job    or os.environ.get("VM_JOB_ID", "local")
    incoming  = Path(args.incoming or os.environ.get("VM_INCOMING", "incoming/chunks"))
    out_dir   = Path(args.out or os.environ.get("VM_OUT", "out"))
    per_gid   = int(args.chunks_per_gid or os.environ.get("VM_CHUNKS_PER_GID",
                                                          DEFAULTS["chunks_per_gid"]))

    indices_raw = args.indices or os.environ.get("VM_INDICES", "")
    if indices_raw.strip():
        indices = [int(x) for x in indices_raw.replace("[", "").replace("]", "")
                   .replace(" ", "").split(",") if x.strip()]
    else:
        indices = sorted(int(p.stem.split("_")[1]) for p in incoming.glob("chunk_*.mp4"))

    out_dir.mkdir(parents=True, exist_ok=True)
    log(f"\n🛠️  Job {job_id} — {len(indices)} morceau(x) sur une machine neuve")

    xgid, gid_counter = generate_xgid(), 0
    ok, failed = [], []

    for idx in indices:
        chunk_path = incoming / f"chunk_{idx:03d}.mp4"
        if not chunk_path.exists():
            failed.append({"idx": idx, "reason": "morceau absent du paquet"})
            log(f"   ⚠️  chunk_{idx:03d} : absent du paquet reçu")
            continue

        if gid_counter >= per_gid:
            xgid, gid_counter = generate_xgid(), 0

        log(f"   📦 chunk_{idx:03d} ({duration_of(chunk_path):.2f}s)")
        try:
            process_chunk(chunk_path, idx, get_headers(xgid), out_dir)
            ok.append(idx)
            log(f"      ✅ OK")
        except VmakeError as e:
            failed.append({"idx": idx, "reason": str(e), "code": str(e.code)})
            log(f"      ❌ {e}")
        except Exception as e:
            failed.append({"idx": idx, "reason": f"{type(e).__name__}: {e}"})
            log(f"      ❌ {type(e).__name__}: {e}")
        gid_counter += 1

    save_state(out_dir, "_manifest.json", {
        "job_id": job_id, "ok": ok, "failed": failed,
        "finished_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
    })
    log(f"\n✅ Job {job_id} terminé — {len(ok)} réussi(s), {len(failed)} échec(s)")

    # On sort en code 0 même en cas d'échec partiel : l'artifact doit remonter
    # pour que le script local sache exactement quels morceaux replacer.
    return 0


# ============================================================
# MODE LOCAL — orchestration multi-comptes
# ============================================================
def build_workers(cfg: dict) -> list:
    """
    Construit la liste des dépôts-travailleurs.

    Le plafond `max_parallel_cap` s'applique PAR COMPTE, pas au total. Sinon
    ajouter des amis ne servirait à rien : avec 3 comptes et un plafond global
    de 12, on croit paralléliser alors que 12 jobs restent 12 jobs. Chaque
    compte a son propre quota GitHub (20 jobs simultanés), donc son propre
    plafond.
    """
    workers = []
    for acc in cfg["_accounts"]:
        per_account = int(acc.parallel_cap or cfg["max_parallel_cap"])
        for n, w in enumerate(acc.workers):
            workers.append({
                "account":    acc,
                "repo":       w,
                "full":       f"{acc.owner}/{w}",
                "slot":       n,
                "per_account": per_account,
                "gh":         GitHub(acc.token, acc.owner, w),
            })
    total = sum(int(a.parallel_cap or cfg["max_parallel_cap"])
                for a in cfg["_accounts"] if a.workers)
    log(f"👥 {len(cfg['_accounts'])} compte(s), {len(workers)} dépôt(s) travailleur(s) "
        f"→ jusqu'à {total} job(s) simultané(s)")
    return workers


def build_chunk_zip(chunks: list, dest: Path) -> Path:
    with zipfile.ZipFile(dest, "w", zipfile.ZIP_DEFLATED) as z:
        for c in chunks:
            z.write(c, arcname=c.name)
    return dest


def ensure_chunks_on_worker(worker: dict, release_tag: str, zip_path: Path, zip_name: str,
                            state: dict, lock):
    """Dépose le paquet de morceaux dans une Release du dépôt, une seule fois."""
    # Le verrou couvre TOUTE l'opération, pas seulement le test : plusieurs jobs
    # d'une même vague partagent le même dépôt. Sans cela, deux threads créent la
    # release puis téléversent l'asset en même temps, et le second reçoit
    # « HTTP 404 » sur l'upload (constaté le 16/10/2026 avec 3 jobs).
    with lock:
        if worker["full"] in state.setdefault("release_ready", []):
            return
        release = worker["gh"].ensure_release(release_tag, f"Morceaux {release_tag}")
        worker["gh"].upload_asset(release, zip_name, zip_path.read_bytes())
        state["release_ready"].append(worker["full"])


def poll_jobs(jobs: list, cfg: dict):
    """Suit les jobs distants jusqu'à leur terme et récupère les artifacts."""
    deadline = time.time() + cfg["job_timeout"]
    pending  = list(jobs)
    interval = cfg.get("poll_interval_seconds", 15)

    while pending and time.time() < deadline:
        time.sleep(interval)

        for job in list(pending):
            gh = job["worker"]["gh"]
            try:
                if job.get("run_id") is None:
                    run = gh.find_run(job["run_name"], timeout=5)
                    if run:
                        job["run_id"] = run["id"]
                    continue

                run = gh.get_run(job["run_id"])
                if run["status"] != "completed":
                    continue

                job["conclusion"] = run["conclusion"]
                if run["conclusion"] == "success":
                    arts = [a for a in gh.list_artifacts(job["run_id"])
                            if a["name"] == job["artifact_name"] and not a.get("expired")]
                    if arts:
                        job["zip"] = gh.download_artifact(arts[0]["id"])
                pending.remove(job)
            except GitHubError as e:
                log(f"   ⚠️  {job['job_id']} : {e}")
                time.sleep(10)

        if pending:
            states = " ".join(
                f"{j['job_id']}:" + ("démarré" if j.get("run_id") else "en file")
                for j in pending
            )
            log(f"   ⏳ {len(pending)} job(s) en cours — {states}")

    for job in pending:
        job["conclusion"] = "timeout"
    return jobs


def extract_results(job: dict, results_dir: Path) -> tuple:
    """Décompresse l'artifact d'un job et place les morceaux traités."""
    if not job.get("zip"):
        return [], [{"idx": i, "reason": "aucun artifact reçu"} for i in job["idxs"]]

    ok, failed = [], []
    tmp = results_dir.parent / f"_tmp_{job['job_id']}"
    tmp.mkdir(parents=True, exist_ok=True)
    try:
        with zipfile.ZipFile(io.BytesIO(job["zip"])) as z:
            z.extractall(tmp)

        manifest = None
        for p in tmp.rglob("_manifest.json"):
            manifest = json.loads(p.read_text(encoding="utf-8"))
            break

        for f in tmp.rglob("chunk_*.mp4"):
            idx = int(f.stem.split("_")[1])
            shutil.copy(f, results_dir / f"chunk_{idx:03d}.mp4")
            ok.append(idx)

        if manifest:
            for item in manifest.get("failed", []):
                if item["idx"] not in ok:
                    failed.append(item)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)

    for i in job["idxs"]:
        if i not in ok and not any(f["idx"] == i for f in failed):
            failed.append({"idx": i, "reason": "morceau non traité"})
    return ok, failed


def orchestrate(cfg: dict, state_dir: Path, video_id: str, chunks: list) -> list:
    """Distribue les morceaux en vagues parallèles sur le pool de comptes."""
    results_dir = state_dir / "results"
    results_dir.mkdir(parents=True, exist_ok=True)

    progress = load_state(state_dir, "progress.json") or {
        "video_id": video_id, "completed": [], "wave": 0, "release_ready": [],
    }
    progress.setdefault("release_ready", [])
    completed = set(progress["completed"])
    remaining = [int(c.stem.split("_")[1]) for c in chunks if int(c.stem.split("_")[1]) not in completed]

    workers   = build_workers(cfg)
    release_tag = f"vmake-{video_id}"
    zip_name    = f"chunks-{video_id}.zip"
    zip_path    = build_chunk_zip(chunks, state_dir / zip_name)
    log(f"📦 Paquet de morceaux : {zip_path.stat().st_size / 1e6:.1f} Mo")

    group = int(cfg["chunks_per_job"])
    wave  = progress["wave"]
    lock  = threading.Lock()

    # Un même dépôt peut héberger plusieurs jobs EN MÊME TEMPS : un compte
    # GitHub gratuit autorise 20 jobs simultanés. On ne se limite donc pas au
    # nombre de dépôts, sinon un seul dépôt = un seul job = 45 min par vidéo.
    #
    # La répartition se fait dans un cycle qui parcourt les comptes un par un.
    # Chaque compte ne reçoit jamais plus que SON plafond : si un compte a un
    # seul dépôt et que son plafond est de 12, ses jobs tournent tous sur ce
    # dépôt. C'est ce qui permet d'avoir plusieurs comptes en parallèle sans
    # qu'un compte lent ou limité bloque les autres.
    def plan(n_jobs: int) -> list:
        """Associe chaque job à un dépôt, en respectant le plafond de chaque compte."""
        use = [0] * len(workers)
        assigned = []
        n = 0
        while len(assigned) < n_jobs:
            progressed = False
            for i, w in enumerate(workers):
                if use[i] >= w["per_account"]:
                    continue
                assigned.append(w)
                use[i] += 1
                progressed = True
                if len(assigned) >= n_jobs:
                    break
            if not progressed:
                break
        return assigned

    while remaining and wave < cfg["max_waves"]:
        wave += 1
        jobs = []
        n_jobs = min(sum(w["per_account"] for w in workers),
                     (len(remaining) + group - 1) // group)
        for n, worker in enumerate(plan(n_jobs)):
            idxs = remaining[n * group:(n + 1) * group]
            if not idxs:
                break
            job_id = f"{video_id[-6:]}-v{wave}j{n + 1}"
            jobs.append({
                "job_id": job_id, "idxs": idxs, "worker": worker,
                "run_name": f"vmake-{job_id}",
                "artifact_name": f"vmake-result-{job_id}",
            })

        log(f"\n{'=' * 60}")
        log(f"🌊 Vague {wave} — {len(jobs)} job(s) en parallèle, {len(remaining)} morceau(x) restant(s)")
        for j in jobs:
            log(f"   • {j['job_id']} → {j['worker']['full']}  morceaux {j['idxs']}")
        log(f"{'=' * 60}")

        # 1. Déposer le paquet dans les Releases concernées (en parallèle)
        with ThreadPoolExecutor(max_workers=len(jobs)) as ex:
            futures = [
                ex.submit(ensure_chunks_on_worker, j["worker"], release_tag, zip_path,
                          zip_name, progress, lock)
                for j in jobs
            ]
            for f in futures:
                try:
                    f.result()
                except GitHubError as e:
                    log(f"   ❌ Préparation du paquet impossible : {e}")

        # 2. Déclencher tous les jobs d'un coup
        for j in jobs:
            payload = {
                "job_id":          j["job_id"],
                "video_id":        video_id,
                "release_tag":     release_tag,
                "chunk_idxs":      j["idxs"],
                "chunks_per_gid":  int(cfg["chunks_per_gid"]),
            }
            try:
                j["worker"]["gh"].dispatch("vmake-chunk", payload)
                log(f"   🚀 {j['job_id']} déclenché sur {j['worker']['full']}")
            except GitHubError as e:
                j["conclusion"] = "dispatch_failed"
                log(f"   ❌ {j['job_id']} : {e}")

        # 3. Attendre et récupérer
        poll_jobs([j for j in jobs if j.get("conclusion") is None], cfg)

        # 4. Comptabiliser
        before = len(remaining)
        for j in jobs:
            if j.get("conclusion") != "success" and not j.get("zip"):
                log(f"   ❌ {j['job_id']} : {j.get('conclusion', 'inconnu')}")
                continue
            ok, failed = extract_results(j, results_dir)
            for i in ok:
                completed.add(i)
                if i in remaining:
                    remaining.remove(i)
            if failed:
                log(f"   ⚠️  {j['job_id']} : {len(failed)} morceau(x) à replacer — "
                    f"{sorted({f.get('reason', '?')[:40] for f in failed})}")

        progress.update({"completed": sorted(completed), "wave": wave,
                         "release_ready": progress["release_ready"]})
        save_state(state_dir, "progress.json", progress)

        gained = before - len(remaining)
        log(f"   📊 Vague {wave} : {gained} morceau(x) récupéré(s), {len(remaining)} restant(s)")

        # Aucun progrès → on réduit la taille des groupes (quota atteint plus tôt que prévu).
        if gained == 0:
            if group > 1:
                group = max(1, group // 2)
                log(f"   🔻 Aucun progrès — taille des groupes réduite à {group}")
            else:
                log("   ⛔ Aucun progrès avec des groupes de 1 morceau. Arrêt.")
                break

    save_state(state_dir, "progress.json", progress)
    zip_path.unlink(missing_ok=True)
    return sorted(completed)


def cleanup_releases(cfg: dict, video_id: str, state_dir: Path):
    progress = load_state(state_dir, "progress.json") or {}
    ready    = progress.get("release_ready", [])
    if not ready:
        return
    log("\n🧹 Nettoyage des paquets de morceaux sur GitHub...")
    for acc in cfg["_accounts"]:
        for w in acc.workers:
            if f"{acc.owner}/{w}" not in ready:
                continue
            try:
                GitHub(acc.token, acc.owner, w).delete_release(f"vmake-{video_id}")
            except GitHubError as e:
                log(f"   ⚠️  {acc.owner}/{w} : {e}")


def run_local(args) -> int:
    _require_ffmpeg()
    cfg = load_accounts(ROOT / "accounts.json")
    if args.chunks_per_job:
        cfg["chunks_per_job"] = args.chunks_per_job
    if args.workers:
        cfg["max_parallel_cap"] = args.workers

    input_path = Path(args.input)
    if not input_path.exists():
        sys.exit(f"❌ Fichier introuvable : {input_path}")

    video_id   = session_for(input_path, args.reset)
    state_dir  = FOLDERS["state"] / video_id
    chunks_dir = state_dir / "chunks"

    log(f"\n🚀 Traitement de « {input_path.name} »")
    log(f"   identifiant de session : {video_id}")
    if args.reset:
        # La session neuve n'existe pas encore : on efface les anciennes traces.
        for vieille in FOLDERS["state"].glob(
                f"{''.join(c for c in input_path.stem if c.isalnum() or c in '-_')[:32]}-*"):
            if vieille.is_dir() and vieille.name != video_id:
                shutil.rmtree(vieille, ignore_errors=True)
        log("   ♻️  état précédent effacé")

    chunks = sorted(chunks_dir.glob("chunk_*.mp4"))
    if not chunks:
        trim = float(cfg.get("vmake_trim", 0.0))
        log(f"\n✂️  Découpage en morceaux de {cfg['chunk_duration']}s (réencodage, images-clés forcées)...")
        if trim:
            log(f"   ↳ compensation du rognage Vmake : +{trim:.3f}s d'avance par morceau")
        chunks = split_video(input_path, chunks_dir, int(cfg["chunk_duration"]),
                             preroll=trim)
        chunks = merge_short_tail(chunks, float(cfg["min_tail_duration"]))
        durations = [duration_of(c) for c in chunks]
        log(f"   ✅ {len(chunks)} morceaux — "
            f"{min(durations):.2f}s à {max(durations):.2f}s "
            f"(total {sum(durations):.2f}s)")
        save_state(state_dir, "meta.json", {
            "video_id": video_id, "input": str(input_path),
            "chunks": [{"idx": int(c.stem.split("_")[1]), "name": c.name,
                        "duration": round(duration_of(c), 3)} for c in chunks],
            "source_duration": duration_of(input_path),
        })
    else:
        log(f"   ♻️  Reprise : {len(chunks)} morceaux déjà découpés")

    total = len(chunks)
    done  = orchestrate(cfg, state_dir, video_id, chunks)

    if not done:
        log("\n❌ Aucun morceau traité. Vérifie tes tokens et l'onglet Actions de tes dépôts.")
        return 1

    if len(done) < total:
        missing = sorted(set(range(total)) - set(done))
        log(f"\n⚠️  {len(missing)} morceau(x) manquant(s) : {missing}")
        log("   La vidéo finale sera plus courte. Relance la même commande pour réessayer.")

    log("\n🎬 Recollage de la vidéo finale...")
    result_files = [state_dir / "results" / f"chunk_{i:03d}.mp4" for i in done
                    if (state_dir / "results" / f"chunk_{i:03d}.mp4").exists()]
    if not result_files:
        log("❌ Aucun résultat exploitable.")
        return 1

    FOLDERS["final"].mkdir(parents=True, exist_ok=True)
    final = FOLDERS["final"] / f"{input_path.stem}_clean.mp4"
    concat_final(result_files, final)

    log(f"\n🎉 Terminé !")
    log(f"   Source : {duration_of(input_path):.2f}s")
    log(f"   Finale : {duration_of(final):.2f}s")
    log(f"   Fichier : {final.resolve()}")

    if not args.keep_releases:
        cleanup_releases(cfg, video_id, state_dir)

    log(f"\n💡 Reprise possible en relançant la même commande (session {video_id}).")
    return 0


# ============================================================
# MAIN
# ============================================================
def main() -> int:
    load_dotenv(ROOT / ".env")

    parser = argparse.ArgumentParser(
        description="Vmake subtitle remover — pipeline multi-comptes GitHub Actions")
    parser.add_argument("--mode", default="local", choices=["local", "action"])
    parser.add_argument("--input", help="vidéo source (mode local)")
    parser.add_argument("--reset", action="store_true",
                        help="repartir de zéro (efface l'état de reprise)")
    parser.add_argument("--workers", type=int, help="plafond de jobs simultanés")
    parser.add_argument("--chunks-per-job", type=int, dest="chunks_per_job",
                        help="morceaux confiés à une machine (défaut 4)")
    parser.add_argument("--keep-releases", action="store_true",
                        help="ne pas supprimer les paquets de morceaux sur GitHub")

    # Options du mode action (renseignées par le workflow GitHub)
    parser.add_argument("--job", help="[action] identifiant du job")
    parser.add_argument("--incoming", help="[action] dossier des morceaux reçus")
    parser.add_argument("--indices", help="[action] indices à traiter, ex. 0,1,2,3")
    parser.add_argument("--out", help="[action] dossier de sortie")
    parser.add_argument("--chunks-per-gid", type=int, dest="chunks_per_gid",
                        help="[action] rotation du x-gid")

    args = parser.parse_args()

    if args.mode == "action":
        return run_action(args)

    if not args.input:
        parser.error("--input est obligatoire en mode local")
    return run_local(args)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        log("\n⏹️  Interrompu. Relance la même commande pour reprendre.")
        sys.exit(130)
    except RuntimeError as e:
        log(f"\n❌ {e}")
        sys.exit(1)
