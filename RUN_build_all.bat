@echo off
setlocal EnableDelayedExpansion
title TradeFinder — Full Platform Build

echo.
echo  ============================================================
echo   TradeFinder — Full Platform Build
echo   Windows  ^>  built locally right now
echo   macOS    ^>  GitHub Actions (macos-latest runner)
echo   Linux    ^>  GitHub Actions (ubuntu-latest runner)
echo  ============================================================
echo.

REM ── Resolve paths relative to this bat file's location ──────────────────────
set ROOT=%~dp0
set CLIENT=%ROOT%electron-client

REM ── Step 1: Build Windows locally ───────────────────────────────────────────
echo [1/3]  Building Windows (local)...
echo.
cd /d "%CLIENT%"
call npm run dist:win
if errorlevel 1 (
    echo.
    echo  ERROR: Windows build failed — aborting.
    pause
    exit /b 1
)

echo.
echo  Windows build complete.
echo  Output: electron-client\dist-exe\TradeFinder.exe
echo.

REM ── Step 2: Read the bumped version from package.json ───────────────────────
for /f "tokens=*" %%v in ('node -p "require('./package.json').version"') do set VERSION=%%v

REM ── Step 3: Commit the version bump and push → triggers CI ─────────────────
echo [2/3]  Pushing version %VERSION% to GitHub to trigger CI...
echo.
cd /d "%ROOT%"

git add electron-client/package.json
git diff --cached --quiet
if errorlevel 1 (
    git commit -m "chore: release v%VERSION%"
) else (
    echo  No changes to commit ^(version already committed^).
)

git push
if errorlevel 1 (
    echo.
    echo  WARNING: git push failed.
    echo  Make sure this folder is a git repo linked to GitHub.
    echo  The Mac and Linux CI builds were NOT triggered.
    echo.
    echo  Quick setup if you haven't done this yet:
    echo    git init
    echo    git remote add origin https://github.com/YOUR_USER/YOUR_REPO.git
    echo    git branch -M main
    echo    git push -u origin main
    echo.
    pause
    exit /b 1
)

REM ── Step 4: Open the GitHub Actions page ────────────────────────────────────
echo.
echo [3/3]  Opening GitHub Actions in browser...
for /f "delims=" %%u in ('git remote get-url origin 2^>nul') do set REMOTE=%%u

REM Strip .git suffix and convert SSH → HTTPS if needed
set REMOTE=!REMOTE:.git=!
set REMOTE=!REMOTE:git@github.com:=https://github.com/!

if not "!REMOTE!"=="" (
    start "" "!REMOTE!/actions"
    echo  Actions page: !REMOTE!/actions
) else (
    echo  Go to your GitHub repo ^> Actions tab to watch the build.
)

echo.
echo  ============================================================
echo   Done!
echo   - Windows EXE is ready in electron-client\dist-exe\
echo   - Mac DMG + Linux AppImage will appear as artifacts
echo     in the Actions run when CI finishes (~5-10 min).
echo  ============================================================
echo.
pause
