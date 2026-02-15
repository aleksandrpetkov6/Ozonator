@echo off
setlocal
cd /d "%~dp0"
if exist "%~dp0release\win-unpacked\Озонатор.exe" (
  start "" "%~dp0release\win-unpacked\Озонатор.exe"
) else (
  echo Portable билд не найден: release\win-unpacked\Озонатор.exe
  echo Сначала соберите приложение: npm run dist
  pause
)
