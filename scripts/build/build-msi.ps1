<#
.SYNOPSIS
  Build CameoDB's Windows installer, cameodb-<version>-x64.msi, on the Windows machine.

.DESCRIPTION
  Builds (or takes) target\release\cameodb.exe, optionally signs it, packages it with
  crates\server\windows\cameodb.wxs, optionally signs the .msi, and stages both for the
  release under dist\<version>\windows\:

    cameodb.exe                      the binary, as the release publishes it
    cameodb.exe.version              what it reports, for build.sh to check
    cameodb-<version>-x64.msi        the installer
    cameodb-<version>-x64.msi.version  the version and the SHA-256 of the cameodb.exe inside,
                                     for build.sh to check it is the one published beside it

  Copy dist\<version>\windows\ to the release machine's dist\<version>\windows\, then run
  scripts/release/build.sh there; it validates all four and the sign stage signs the .exe
  and .msi with cosign like every other artifact.

  Needs: Rust (rust-toolchain.toml pins the version), the MSVC build tools (docs/BUILDING.md),
  and WiX Toolset v6:  dotnet tool install --global wix
  Signing needs Windows SDK's signtool and a code-signing certificate in the certificate store
  or on its token.

.PARAMETER Build
  Build target\release\cameodb.exe first. Without it, an existing build is used.

.PARAMETER CertificateThumbprint
  Sign cameodb.exe and the .msi with this certificate (Authenticode, SHA-256, timestamped).
  Unsigned without it: the installer works, Windows names its publisher "Unknown".

.PARAMETER TimestampUrl
  RFC 3161 timestamp server for the signatures (default: DigiCert's).

.EXAMPLE
  .\scripts\build\build-msi.ps1 -Build
.EXAMPLE
  .\scripts\build\build-msi.ps1 -Build -CertificateThumbprint 0123456789ABCDEF0123456789ABCDEF01234567
#>
[CmdletBinding()]
param(
    [switch]$Build,
    [string]$CertificateThumbprint,
    [string]$TimestampUrl = "http://timestamp.digicert.com"
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
Set-Location $RepoRoot

function Fail([string]$Message) {
    Write-Host "error: $Message" -ForegroundColor Red
    exit 1
}

function Step([string]$Message) {
    Write-Host "==> $Message" -ForegroundColor Cyan
}

# ---------------------------------------------------------------- version

$manifest = Get-Content "crates\server\Cargo.toml" -Raw
if ($manifest -notmatch '(?m)^version\s*=\s*"([0-9]+\.[0-9]+\.[0-9]+)"') {
    Fail "no major.minor.patch version in crates\server\Cargo.toml; an MSI version cannot carry a pre-release suffix"
}
$Version = $Matches[1]
$Dist = Join-Path $RepoRoot "dist\$Version\windows"
$Msi = Join-Path $Dist "cameodb-$Version-x64.msi"
Step "CameoDB $Version"

# ---------------------------------------------------------------- binary

$Exe = Join-Path $RepoRoot "target\release\cameodb.exe"
if ($Build) {
    Step "cargo build --release -p server"
    cargo build --release -p server
    if ($LASTEXITCODE -ne 0) { Fail "cargo build failed" }
}
if (-not (Test-Path $Exe)) { Fail "no $Exe; run with -Build" }

$reported = (& $Exe --version | Out-String).Trim()
if ($reported -like "*+fault-injection*") {
    Fail "$Exe reports '$reported': a fault-injection build carries panic seams a release must not; rebuild with -Build"
}
if ($reported -ne "cameodb $Version") {
    Fail "$Exe reports '$reported', not 'cameodb $Version'; rebuild with -Build"
}

# ---------------------------------------------------------------- signing

$SignTool = $null
if ($CertificateThumbprint) {
    $SignTool = Get-Command signtool.exe -ErrorAction SilentlyContinue
    if (-not $SignTool) {
        $sdk = Get-ChildItem "${env:ProgramFiles(x86)}\Windows Kits\10\bin\*\x64\signtool.exe" -ErrorAction SilentlyContinue |
            Sort-Object FullName -Descending | Select-Object -First 1
        if (-not $sdk) { Fail "signtool.exe not found: install the Windows SDK" }
        $SignTool = $sdk.FullName
    }
}

function Sign([string]$Path) {
    if (-not $SignTool) { return }
    Step "signtool sign $(Split-Path $Path -Leaf)"
    & $SignTool sign /sha1 $CertificateThumbprint /fd sha256 /tr $TimestampUrl /td sha256 $Path
    if ($LASTEXITCODE -ne 0) { Fail "signing $Path failed" }
    & $SignTool verify /pa $Path
    if ($LASTEXITCODE -ne 0) { Fail "the signature on $Path does not verify" }
}

# The .exe is signed before it is packaged, so the one inside the MSI is the signed one too:
# Windows checks the program each time the service starts, not only the installer.
New-Item -ItemType Directory -Force -Path $Dist | Out-Null
$StagedExe = Join-Path $Dist "cameodb.exe"
Copy-Item $Exe $StagedExe -Force
Sign $StagedExe

# ---------------------------------------------------------------- installer

$wix = Get-Command wix -ErrorAction SilentlyContinue
if (-not $wix) { Fail "WiX Toolset not found: dotnet tool install --global wix" }

Step "wix build -> $(Split-Path $Msi -Leaf)"
wix build "crates\server\windows\cameodb.wxs" `
    -arch x64 `
    -d "Version=$Version" `
    -d "BinDir=$Dist" `
    -d "RepoRoot=$RepoRoot" `
    -o $Msi
if ($LASTEXITCODE -ne 0) { Fail "wix build failed" }
Sign $Msi

# ---------------------------------------------------------------- sidecars

# ASCII, not PowerShell 5.1's UTF-16 default, though build.sh tolerates both.
$exeHash = (Get-FileHash $StagedExe -Algorithm SHA256).Hash.ToLowerInvariant()
Set-Content -Path "$StagedExe.version" -Value $reported -Encoding ascii
Set-Content -Path "$Msi.version" -Encoding ascii -Value @(
    "cameodb $Version",
    "cameodb.exe sha256 $exeHash"
)
# Packaging intermediates WiX leaves beside the output; only the .msi is an artifact.
Remove-Item (Join-Path $Dist "*.wixpdb") -ErrorAction SilentlyContinue

Step "staged in $Dist"
Get-ChildItem $Dist | ForEach-Object { Write-Host ("    {0,-36} {1,12:N0} bytes" -f $_.Name, $_.Length) }
if (-not $SignTool) {
    Write-Host "unsigned: Windows will show the publisher as Unknown (see docs/DEPLOYMENT.md)" -ForegroundColor Yellow
}
