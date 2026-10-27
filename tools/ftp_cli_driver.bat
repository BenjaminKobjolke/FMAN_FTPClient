@echo off
python "%~dp0ftp_cli_driver.py" %*
exit /b %ERRORLEVEL%
