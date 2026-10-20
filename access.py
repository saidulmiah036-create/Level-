import requests, json, sys, time, os, asyncio
from getpass import getpass
from typing import Optional, Callable, Any
import httpx

# Shared configuration used by both the CLI tester and the bot fallback.
CLIENT_ID = "100067"
CLIENT_SECRET = os.environ.get(
    "GARENA_CLIENT_SECRET",
    "2ee44819e9b4598845141067b281621874d0d5d7af9d8f7e00c1e54715b7d1e3",
)
ENDPOINTS = (
    "https://auth.garena.com/oauth/guest/token/grant",
    "https://100067.connect.garena.com/oauth/guest/token/grant",
)
TIMEOUT = 10


async def get_guest_access_token(
    client: "httpx.AsyncClient",
    uid: str,
    password: str,
    logger: Optional[Callable[[str], Any]] = None,
):
    """Return (open_id, access_token, platform) using endpoint failover.

    The helper is safe to import: it does not prompt or print response bodies.
    It tries each configured endpoint in order, at most twice per endpoint,
    and accepts only a JSON response with non-empty open_id and access_token.
    """
    def log(message: str) -> None:
        if logger is not None:
            try:
                logger(message)
            except Exception:
                pass

    uid = str(uid).strip()
    if not uid or not password:
        log("[LOGIN] Guest-token fallback skipped: UID/password is empty")
        return None

    payload = {
        "uid": uid,
        "password": str(password),
        "response_type": "token",
        "client_type": "2",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
    }
    headers = {
        "User-Agent": "Dalvik/2.1.0 (Linux; U; Android 12; SM-G998B Build/SP1A.210812.016)",
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "close",
    }

    attempt = 0
    total_attempts = len(ENDPOINTS) * 2
    for endpoint in ENDPOINTS:
        for endpoint_try in range(1, 3):
            attempt += 1
            try:
                response = await client.post(
                    endpoint, headers=headers, data=payload, timeout=TIMEOUT
                )
            except asyncio.CancelledError:
                raise
            except httpx.TimeoutException:
                log(f"[LOGIN] Token endpoint timed out ({attempt}/{total_attempts}): {endpoint}")
            except httpx.RequestError as exc:
                log(f"[LOGIN] Token endpoint connection error ({attempt}/{total_attempts}): {type(exc).__name__}")
            except Exception as exc:
                log(f"[LOGIN] Token endpoint request failed ({attempt}/{total_attempts}): {type(exc).__name__}")
            else:
                response_data = {}
                if 200 <= response.status_code < 300:
                    try:
                        response_data = response.json()
                    except (ValueError, TypeError):
                        log(f"[LOGIN] Token endpoint returned invalid JSON: {endpoint}")

                    if isinstance(response_data, dict):
                        open_id = response_data.get("open_id")
                        access_token = response_data.get("access_token")
                        if (
                            isinstance(open_id, (str, int))
                            and str(open_id).strip()
                            and isinstance(access_token, str)
                            and access_token.strip()
                        ):
                            platform = response_data.get("platform", 4)
                            log(f"[LOGIN] Access token obtained from fallback endpoint: {endpoint}")
                            return str(open_id), access_token, platform
                        error_text = (
                            response_data.get("error_description")
                            or response_data.get("error")
                            or response_data.get("message")
                            or "missing open_id/access_token"
                        )
                        log(
                            f"[LOGIN] Token endpoint HTTP {response.status_code} "
                            f"without a usable token ({endpoint}): {str(error_text)[:160]}"
                        )
                    else:
                        log(f"[LOGIN] Token endpoint returned an unexpected JSON type: {endpoint}")
                elif response.status_code == 429:
                    log(f"[LOGIN] Token endpoint rate-limited ({attempt}/{total_attempts}): {endpoint}")
                else:
                    log(f"[LOGIN] Token endpoint HTTP {response.status_code} ({attempt}/{total_attempts}): {endpoint}")

            if endpoint_try < 2:
                await asyncio.sleep(0.5)

    log("[LOGIN] All guest-token endpoints failed to return a usable access token")
    return None
from typing import Optional, Callable, Any
import httpx

R="\033[0m"; B="\033[1m"; D="\033[2m"
CY="\033[96m"; GR="\033[92m"; YL="\033[93m"; RD="\033[91m"; MG="\033[95m"; BL="\033[94m"

def clear(): os.system("cls" if os.name=="nt" else "clear")

def line(c="─", n=54, col=CY): print(f"{col}{c*n}{R}")

def banner():
    clear()
    print()
    line("═")
    print(f"{B}{MG}        ✦  GARENA GUEST TOKEN TESTER  ✦{R}")
    print(f"{D}{CY}             kiểm tra endpoint token{R}")
    line("═")
    print()

def spin(msg, t=0.6):
    frames="⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"
    end=time.time()+t
    i=0
    while time.time()<end:
        sys.stdout.write(f"\r{YL}{frames[i%len(frames)]}{R} {msg}")
        sys.stdout.flush()
        time.sleep(0.05)
        i+=1
    sys.stdout.write("\r"+" "*60+"\r")

def pretty(txt):
    try:
        return json.dumps(json.loads(txt), indent=2, ensure_ascii=False)
    except Exception:
        return txt

def colorize_json(txt):
    out=[]
    for ln in txt.splitlines():
        s=ln.strip()
        if s.startswith('"') and '":' in s:
            k,_,v=s.partition(":")
            out.append(f"  {CY}{k}{R}:{GR}{v}{R}")
        elif s.startswith(("{","}","[","]")):
            out.append(f"{D}{ln}{R}")
        else:
            out.append(f"{GR}{ln}{R}")
    return "\n".join(out)

def field(label, value, col=CY):
    print(f"  {col}▸{R} {B}{label:<10}{R} {D}{value}{R}")

def main():
    banner()
    print(f"{B}{BL}  ╭─ NHẬP THÔNG TIN ─────────────────────────╮{R}")
    uid=input(f"  {CY}│{R} {B}UID      {R}❯ ").strip()
    pwd=getpass(f"  {CY}│{R} {B}PASSWORD {R}❯ ")
    print(f"{B}{BL}  ╰──────────────────────────────────────────╯{R}")
    print()
    if not uid or not pwd:
        print(f"{RD}  ✖ UID hoặc Password trống, thoát.{R}\n")
        return

    urls = ENDPOINTS
    payload={
        "uid":uid,"password":pwd,
        "response_type":"token","client_type":"2",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
    }

    print(f"{B}{MG}  ⚡ BẮT ĐẦU KIỂM TRA {len(urls)} ENDPOINT{R}")
    line()
    ok=0
    for i,u in enumerate(urls,1):
        print()
        print(f"{B}{CY}  [{i}/{len(urls)}] {R}{B}{u}{R}")
        spin(f"đang gửi request...")
        try:
            t0=time.time()
            r=requests.post(u, data=payload, timeout=TIMEOUT)
            dt=(time.time()-t0)*1000
            sc=r.status_code
            col=GR if 200<=sc<300 else (YL if 400<=sc<500 else RD)
            ic="✔" if 200<=sc<300 else ("⚠" if 400<=sc<500 else "✖")
            if 200<=sc<300: ok+=1
            field("STATUS", f"{col}{ic} {sc}{R}  {D}({dt:.0f}ms){R}")
            body=pretty(r.text)
            print(f"  {CY}▸{R} {B}BODY{R}")
            print(f"{D}  ┌──────────────────────────────────────────{R}")
            for ln in colorize_json(body).splitlines():
                print(f"{D}  │{R} {ln}")
            print(f"{D}  └──────────────────────────────────────────{R}")
        except requests.exceptions.RequestException as e:
            field("ERROR", f"{RD}{e}{R}")
    print()
    line("═")
    col=GR if ok==len(urls) else (YL if ok>0 else RD)
    print(f"{B}  KẾT QUẢ: {col}{ok}/{len(urls)} endpoint thành công{R}")
    line("═")
    print()

if __name__=="__main__":
    try:
        main()
    except KeyboardInterrupt:
        print(f"\n{YL}  ⏹ Đã hủy bởi người dùng.{R}\n")