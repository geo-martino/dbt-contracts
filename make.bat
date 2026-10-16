@ECHO OFF

pushd %~dp0

REM Development commands, plus the Sphinx documentation targets

if "%SPHINXBUILD%" == "" (
	set SPHINXBUILD=sphinx-build
)
set SOURCEDIR=docs
set BUILDDIR=docs/_build

REM development commands run through uv so don't need Sphinx on the PATH
if "%1" == "install" goto install
if "%1" == "test" goto test
if "%1" == "lint" goto lint
if "%1" == "lint-fix" goto lint-fix
if "%1" == "docs" goto docs
if "%1" == "check" goto check

%SPHINXBUILD% >NUL 2>NUL
if errorlevel 9009 (
	echo.
	echo.The 'sphinx-build' command was not found. Make sure you have Sphinx
	echo.installed, then set the SPHINXBUILD environment variable to point
	echo.to the full path of the 'sphinx-build' executable. Alternatively you
	echo.may add the Sphinx directory to PATH.
	echo.
	echo.If you don't have Sphinx installed, grab it from
	echo.https://www.sphinx-doc.org/
	exit /b 1
)

if "%1" == "" goto help
if "%1" == "rebuild-html" goto rebuild-html

%SPHINXBUILD% -M %1 %SOURCEDIR% %BUILDDIR% %SPHINXOPTS% %O%
goto end

:help
echo.Development commands:
echo.  install       Install the package with all dependency groups
echo.  test          Run the tests
echo.  lint          Check the code with ruff
echo.  lint-fix      Check the code with ruff and fix what it can
echo.  docs          Build the HTML documentation, failing on any warnings
echo.  check         Run lint, test and docs i.e. everything CI validates
echo.  rebuild-html  Clean and rebuild the HTML documentation, then check its links
echo.
%SPHINXBUILD% -M help %SOURCEDIR% %BUILDDIR% %SPHINXOPTS% %O%
goto end

:install
uv sync
goto end

:test
uv run pytest -n auto -m "not manual"
goto end

:lint
uv run ruff check .
goto end

:lint-fix
uv run ruff check --fix .
goto end

:docs
uv run python %SOURCEDIR%/contracts.py || goto end
uv run %SPHINXBUILD% -M html %SOURCEDIR% %BUILDDIR% %SPHINXOPTS% -W --keep-going %O%
goto end

:check
uv run ruff check . || goto end
uv run pytest -n auto -m "not manual" || goto end
uv run python %SOURCEDIR%/contracts.py || goto end
uv run %SPHINXBUILD% -M html %SOURCEDIR% %BUILDDIR% %SPHINXOPTS% -W --keep-going %O%
goto end

:rebuild-html
%SPHINXBUILD% -M clean %SOURCEDIR% %BUILDDIR% %SPHINXOPTS% %O%
if exist %SOURCEDIR%\reference\api rmdir /s /q %SOURCEDIR%\reference\api
%SPHINXBUILD% -M html %SOURCEDIR% %BUILDDIR% %SPHINXOPTS% %O%
%SPHINXBUILD% -b linkcheck %SOURCEDIR% %BUILDDIR% %SPHINXOPTS% %O%
goto end

:end
popd
