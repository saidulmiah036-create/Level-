# ROCKS deployment and anonymous per-browser profiles

## Railway persistent volume

1. Open the Railway project and select the ROCKS service.
2. Open **Settings → Volumes → Add Volume**.
3. Set the mount path to `/data` and attach it to the service.
4. Redeploy/restart the service. ROCKS uses `RAILWAY_VOLUME_MOUNT_PATH` automatically.
5. Open `/health`; `data_directory_configured: true` means the mounted path was detected.

Do not only set `ROCKS_DATA_DIR=/data` without attaching a real persistent volume. A normal container directory is still ephemeral.

## Dashboard behavior

- Dashboard username/password sign-in, seeded dashboard users, and dashboard user-management endpoints/UI have been removed.
- Each browser receives a random HttpOnly `rocks_profile` cookie. That browser's account records are stored in its own `accounts_guest_<hash>.json` file under `DATA_DIR`.
- Different browsers receive different profile cookies and see separate account lists/statuses. Clearing browser cookies creates a new anonymous profile; the previous profile file remains on disk.
- This is anonymous per-browser separation, not verified identity. Anyone with a copied profile cookie can use that profile. Keep the service private if strong access control is required.
- The UID/password fields in the **Guest** account form are game-account credentials used by that login method. They are not dashboard usernames/passwords. Access-token login remains available.
- Existing legacy rows without an owner remain under the legacy owner and are not exposed in anonymous profiles by default. Add accounts from the browser profile that should own them.
- Root `accounts.json` is an optional seed file. `.dockerignore` excludes `accounts_<profile>.json` and other runtime credentials from the image, while allowing root `accounts.json` as the optional seed. Keep the repository private if it contains real credentials; preferably move credentials into the attached volume.

## Storage details

- Storage path priority: `ROCKS_DATA_DIR` → `RAILWAY_VOLUME_MOUNT_PATH` → project directory (local-development fallback).
- `index.html` remains in the project root; no `templates/` directory is required.
- Existing JSON data is copied into a newly configured data directory only when the destination does not already exist. Existing volume data is not overwritten.
- Writes use unique temporary files and atomic replacement.
- If the host already deleted ephemeral files in a previous redeploy, code changes cannot recover them; restore a backup or add the accounts again.

## Account status and isolation

- Runtime records, credential aliases, and worker registry entries remain scoped by the owning anonymous browser profile.
- A configured account with unresolved login status appears as `LOGIN PENDING`; a successfully logged-in account with no active match appears as `ONLINE IDLE`; an active match appears as `IN_MATCH`.
- Pause, restart, delete, delete-all, import, mode switching and log clearing operate within the current profile's ownership scope.
- Ban-check results are refreshed periodically. An inconclusive response is shown as unknown rather than guessed.

## Validation

- Python compilation, inline JavaScript syntax, ZIP integrity, app route creation, anonymous dashboard access, and separate-browser profile-file mapping were tested locally.
- No live Railway deployment or upstream game authentication was performed from this environment.


Dashboard UI note: the persistent-storage warning banner has been removed from the page. The `/health` endpoint and server startup diagnostics still report whether a persistent volume is detected; removing the banner does not create or attach a volume.
