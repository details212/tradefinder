@echo off
setlocal EnableDelayedExpansion
title TradeFinder — Trigger CI Build

echo.
echo  ============================================================
echo   Trigger GitHub Actions (macOS + Linux + Windows)
echo   No local build — CI does everything.
echo  ============================================================
echo.

set ROOT=%~dp0

cd /d "%ROOT%"

REM Check for uncommitted changes; if there are none we push an empty
REM trigger commit so the workflow fires even with no code changes.
git status --porcelain > nul 2>&1
if errorlevel 1 (
    echo  ERROR: This folder is not a git repository.
    echo  Run RUN_build_all.bat first to set things up.
    pause
    exit /b 1
)

for /f "tokens=*" %%c in ('git status --porcelain') do set HAS_CHANGES=%%c

if not defined HAS_CHANGES (
    echo  No local changes — creating an empty trigger commit...
    git commit --allow-empty -m "ci: trigger build"
)

git push
if errorlevel 1 (
    echo.
    echo  ERROR: git push failed.
    echo  Make sure a remote named 'origin' is configured.
    pause
    exit /b 1
)

echo.
echo  Push complete. GitHub Actions will now build for all platforms.
echo.

for /f "delims=" %%u in ('git remote get-url origin 2^>nul') do set REMOTE=%%u
set REMOTE=!REMOTE:.git=!
set REMOTE=!REMOTE:git@github.com:=https://github.com/!

if not "!REMOTE!"=="" (
    start "" "!REMOTE!/actions"
    echo  Actions page: !REMOTE!/actions
)

echo.
pause
