@echo off
uv pip install --python-version 3.14 --python-platform x86_64-pc-windows-msvc --target "%~dp0..\ftpclient\vendor-import" --no-deps --only-binary=:all: pycryptodome==4.2.0
if errorlevel 1 exit /b %ERRORLEVEL%
uv pip install --python-version 3.14 --python-platform x86_64-pc-windows-msvc --target "%~dp0..\ftpclient\vendor-import" --no-deps "python-ftp-account-readers @ git+https://github.com/BenjaminKobjolke/python-ftp-account-readers.git@5924127ff51838c9e340812a5858e2774f4d779b"
if errorlevel 1 exit /b %ERRORLEVEL%
exit /b 0
