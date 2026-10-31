@echo off
setlocal
set "CONFIG_NAME=tools/ftp_ui_driver_config.bat"
set "CONFIG=%~dp0ftp_ui_driver_config.bat"
if not exist "%CONFIG%" (
  echo {"error":"Missing %CONFIG_NAME%; copy tools/ftp_ui_driver_config.example.bat and set FMAN_SOURCE_DIR."}
  exit /b 1
)
call "%CONFIG%"
if not defined FMAN_SOURCE_DIR (
  echo {"error":"Set FMAN_SOURCE_DIR in %CONFIG_NAME%."}
  exit /b 1
)
if not exist "%~dp0ftp_ui_driver\ftp_ui_driver.py" (
  echo {"error":"Driver script is missing; reinstall this plugin checkout. Configuration: %CONFIG_NAME%."}
  exit /b 1
)
if not exist "%FMAN_SOURCE_DIR%\tools\find_build_python.bat" (
  echo {"error":"FMAN_SOURCE_DIR is not an fman checkout; check %CONFIG_NAME%."}
  exit /b 1
)
if not exist "%FMAN_SOURCE_DIR%\tools\fman_ui\fman_ui.py" (
  echo {"error":"fman UI driver is missing from FMAN_SOURCE_DIR; check %CONFIG_NAME%."}
  exit /b 1
)
if not exist "%FMAN_SOURCE_DIR%\.venv\Scripts\python.exe" (
  echo {"error":"The fman checkout .venv is missing; run its install.bat. Configuration: %CONFIG_NAME%."}
  exit /b 1
)
set "VIRTUAL_ENV="
call "%FMAN_SOURCE_DIR%\tools\find_build_python.bat"
if errorlevel 1 (
  echo {"error":"Could not find the fman checkout Python; check its .venv and %CONFIG_NAME%."}
  exit /b 1
)
"%FMAN_PYTHON%" "%~dp0ftp_ui_driver\ftp_ui_driver.py" %*
set "RESULT=%ERRORLEVEL%"
endlocal & exit /b %RESULT%
