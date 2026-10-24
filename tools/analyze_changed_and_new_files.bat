@echo off
if not exist "%~dp0analyze_code_config.bat" (
    echo ERROR: analyze_code_config.bat not found.
    echo Copy analyze_code_config.example.bat to analyze_code_config.bat and set CLI_ANALYZER_PATH and LANGUAGE.
    exit /b 1
)
call "%~dp0analyze_code_config.bat"
if errorlevel 1 exit /b %ERRORLEVEL%
if not defined CLI_ANALYZER_PATH (
    echo ERROR: CLI_ANALYZER_PATH is not set.
    exit /b 1
)
if not defined LANGUAGE (
    echo ERROR: LANGUAGE is not set.
    exit /b 1
)
cd /d "%~dp0.."
"%CLI_ANALYZER_PATH%\venv\Scripts\python.exe" "%CLI_ANALYZER_PATH%\main.py" --language %LANGUAGE% --path "." --only-changed --fail-on-findings --verbosity minimal --output "code_analysis_results" --rules "code_analysis_rules.json"
exit /b %ERRORLEVEL%
