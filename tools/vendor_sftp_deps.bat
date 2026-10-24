@echo off
uv pip install --python-version 3.14 --python-platform x86_64-pc-windows-msvc --target "%~dp0..\ftpclient\vendor-sftp" --no-deps --only-binary=:all: ^
  paramiko==5.1.0 ^
  cryptography==50.0.2 ^
  bcrypt==5.0.0 ^
  pynacl==1.6.2 ^
  cffi==2.1.1 ^
  pycparser==3.12 ^
  invoke==3.0.3
exit /b %ERRORLEVEL%
