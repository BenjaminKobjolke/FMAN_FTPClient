@echo off
uv pip install --python-version 3.14 --target "%~dp0..\test-vendor" --no-deps ^
  pyftpdlib==2.2.0 ^
  pyasyncore==1.0.5 ^
  pyasynchat==1.0.5 ^
  pyOpenSSL==26.4.0
exit /b %ERRORLEVEL%
