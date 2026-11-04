<#
.SYNOPSIS
  Build, test and verify CameoDB's Windows release in one pass, on the Windows machine.

.DESCRIPTION
  The Windows half of RELEASE-CHECKLIST.md (steps 3-5 and 8) as one command. Stages, in order:

    preflight   the tools are there and the tree is what is being released: git commit and a
                clean tree, the pinned Rust toolchain, the MSVC build tools, and for the msi stage
                .NET, WiX and its Util extension; the six crates agree on the version
    build       cargo build --release -p server, then --version must read this release and not
                carry +fault-injection
    test        cargo test --workspace --no-fail-fast (debug profile, so target\release\cameodb.exe is not
                replaced by a test build)
    validate    scripts\validate\windows.ps1: smoke, data-dir (OB30), remote-sources, and
                the optional docker checks when -DockerImage is given
    msi         scripts\build\build-msi.ps1 (no rebuild): stages cameodb.exe, the installer and
                their .version sidecars in dist\<version>\windows\, after checking that the
                cameodb.exe built and validated above is still the one on disk

  Everything printed is also written to dist\<version>\windows-verify\verify-<commit>.log, which is
  the evidence to attach to the release record. It is kept out of windows\ because publish.sh
  copies everything in that folder to the download site. A stage that fails stops the run, except test and
  validate. A failed test stage does not stop validate or msi either: build.sh treats it as a
  warning, so the record carries it to the release machine instead.

  Afterwards, copy dist\<version>\windows\ to dist\<version>\windows\ on the release machine and
  run scripts/release/release.sh --stage build there, as the release README describes.

  Needs what docs/BUILDING.md describes. This script is not signed; if PowerShell refuses to
  run it: Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass

.PARAMETER Stage
  Stages to run, in the order above. Default: all.

.PARAMETER SkipTests
  Leave out the test stage (cargo test --workspace takes the longest).

.PARAMETER DockerImage
  Optional; not part of a release. A Linux CameoDB image for extra Docker Desktop checks in the
  validate stage. See validate\windows.ps1.

.PARAMETER CertificateThumbprint
  Sign cameodb.exe and the .msi with this certificate; passed to build-msi.ps1.

.PARAMETER AllowDirty
  Go on with uncommitted changes in the tree, which the log records. A release is built from a
  clean checkout of the release commit.

.EXAMPLE
  .\scripts\build\windows-release.ps1
.EXAMPLE
  .\scripts\build\windows-release.ps1 -Stage build,validate -DockerImage cameodb:win-check
.EXAMPLE
  .\scripts\build\windows-release.ps1 -SkipTests -CertificateThumbprint 0123456789ABCDEF0123456789ABCDEF01234567
#>
[CmdletBinding()]
param(
    [ValidateSet("preflight", "build", "test", "validate", "msi")]
    [string[]]$Stage = @("preflight", "build", "test", "validate", "msi"),
    [switch]$SkipTests,
    [string]$DockerImage,
    [string]$CertificateThumbprint,
    [switch]$AllowDirty
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
Set-Location $RepoRoot
$Exe = Join-Path $RepoRoot "target\release\cameodb.exe"

if ($SkipTests) { $Stage = @($Stage | Where-Object { $_ -ne "test" }) }
$Run = { param($name) $Stage -contains $name }

function Step([string]$Message) { Write-Host ""; Write-Host "==> $Message" -ForegroundColor Cyan }
function Ok([string]$Message) { Write-Host "  ok   " -ForegroundColor Green -NoNewline; Write-Host $Message }
function Warn([string]$Message) { Write-Host "  warn " -ForegroundColor Yellow -NoNewline; Write-Host $Message }
function Bad([string]$Message) { Write-Host "  FAIL " -ForegroundColor Red -NoNewline; Write-Host $Message }

# A native command's exit code, with its output shown. PowerShell 5.1 turns a native command's
# stderr into error records under $ErrorActionPreference = "Stop"; cargo writes progress there.
function Invoke-Tool([string]$File, [string[]]$Arguments) {
    $previous = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        & $File @Arguments 2>&1 | ForEach-Object { "$_" } | Write-Host
        $code = $LASTEXITCODE
    } finally { $ErrorActionPreference = $previous }
    $code
}

function Get-ToolOutput([string]$File, [string[]]$Arguments) {
    $previous = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try { (& $File @Arguments 2>&1 | Out-String).Trim() } finally { $ErrorActionPreference = $previous }
}

# ---------------------------------------------------------------- version and commit

$versions = @(Get-ChildItem "crates\*\Cargo.toml" | ForEach-Object {
    if ((Get-Content $_.FullName -Raw) -match '(?m)^version\s*=\s*"([^"]+)"') { $Matches[1] }
} | Sort-Object -Unique)
if ($versions.Count -ne 1) { Write-Host "error: the crates disagree on the version: $($versions -join ', ')" -ForegroundColor Red; exit 1 }
$Version = $versions[0]

$Commit = "unknown"
$FullCommit = $null
$Dirty = $false
if (Get-Command git -ErrorAction SilentlyContinue) {
    $sha = Get-ToolOutput git @("rev-parse", "--short=12", "HEAD")
    if ($sha -match '^[0-9a-f]{7,}$') { $Commit = $sha; $FullCommit = Get-ToolOutput git @("rev-parse", "HEAD") }
    $Dirty = [bool](Get-ToolOutput git @("status", "--porcelain"))
}

$Dist = Join-Path $RepoRoot "dist\$Version\windows"
# Not under windows\: publish.sh copies everything in that folder to the download site.
$LogDir = Join-Path $RepoRoot "dist\$Version\windows-verify"
New-Item -ItemType Directory -Force -Path $Dist, $LogDir | Out-Null
$LogFile = Join-Path $LogDir "verify-$Commit.log"
Start-Transcript -Path $LogFile -Force | Out-Null

$Results = [ordered]@{}
$Failed = $false
# A failed test stage is reported and fails the run's exit code, but does not stop validate or
# msi: build.sh on the release machine treats it as a warning, not a refusal.
$TestsFailed = $false

try {
    Write-Host "CameoDB $Version  commit $Commit$(if ($Dirty) { ' (uncommitted changes)' })  on $env:COMPUTERNAME"
    Write-Host "stages: $($Stage -join ', ')"

    # ------------------------------------------------------------ preflight
    if (& $Run "preflight") {
        Step "preflight"
        $problems = 0

        if ($Dirty -and -not $AllowDirty) { Bad "the tree has uncommitted changes; commit them, or pass -AllowDirty to build anyway"; $problems++ }
        elseif ($Dirty) { Warn "building with uncommitted changes (-AllowDirty); the log records it" }
        else { Ok "clean tree at $Commit" }

        $pin = $null
        if ((Get-Content "rust-toolchain.toml" -Raw) -match '(?m)^channel\s*=\s*"([^"]+)"') { $pin = $Matches[1] }
        if (-not (Get-Command rustc -ErrorAction SilentlyContinue)) { Bad "rustc not found; install Rust through rustup (docs/BUILDING.md)"; $problems++ }
        else {
            $rustc = Get-ToolOutput rustc @("--version")
            if ($pin -and ($rustc -notmatch [regex]::Escape($pin))) { Bad "rustc is '$rustc', the tree pins $pin"; $problems++ }
            else { Ok "$rustc (pinned: $pin)" }
            $target = Get-ToolOutput rustc @("-vV")
            if ($target -notmatch 'host: x86_64-pc-windows-msvc') { Bad "the Rust host is not x86_64-pc-windows-msvc"; $problems++ }
        }

        $vswhere = Join-Path ${env:ProgramFiles(x86)} "Microsoft Visual Studio\Installer\vswhere.exe"
        $msvc = if (Test-Path $vswhere) { Get-ToolOutput $vswhere @("-latest", "-products", "*", "-requires", "Microsoft.VisualStudio.Component.VC.Tools.x86.x64", "-property", "installationPath") } else { "" }
        if ($msvc) { Ok "MSVC build tools in $msvc" } else { Bad "the MSVC build tools (Desktop development with C++) were not found; docs/BUILDING.md"; $problems++ }

        if (& $Run "msi") {
            if (Get-Command dotnet -ErrorAction SilentlyContinue) { Ok ".NET SDK $(Get-ToolOutput dotnet @('--version'))" } else { Bad ".NET SDK not found: winget install Microsoft.DotNet.SDK.8"; $problems++ }
            if (Get-Command wix -ErrorAction SilentlyContinue) {
                $wixVersion = ((Get-ToolOutput wix @("--version")) -split '\+')[0]
                Ok "WiX $wixVersion"
                if ((Get-ToolOutput wix @("extension", "list", "-g")) -match "WixToolset.Util.wixext") { Ok "WiX Util extension" }
                else { Bad "WiX Util extension missing: wix extension add -g WixToolset.Util.wixext/$wixVersion"; $problems++ }
            } else { Bad "WiX not found: dotnet tool install --global wix"; $problems++ }
            if ($CertificateThumbprint -and -not (Get-ChildItem "Cert:\CurrentUser\My", "Cert:\LocalMachine\My" -ErrorAction SilentlyContinue | Where-Object Thumbprint -eq $CertificateThumbprint)) {
                Bad "no certificate with thumbprint $CertificateThumbprint in the certificate stores"; $problems++
            }
        }
        if ($DockerImage -and (& $Run "validate")) {
            if (Get-Command docker -ErrorAction SilentlyContinue) { Ok "docker found (the image is checked by the validate stage)" } else { Bad "-DockerImage given but docker is not on PATH"; $problems++ }
        }

        $free = [math]::Round((Get-PSDrive -Name ($RepoRoot.Substring(0, 1))).Free / 1GB, 1)
        if ($free -lt 10) { Warn "$free GB free on the build drive; a release build and the tests want roughly 10" } else { Ok "$free GB free" }

        $Results["preflight"] = if ($problems -eq 0) { "PASS" } else { "FAIL ($problems)" }
        if ($problems -gt 0) { $Failed = $true; throw "preflight found $problems problem(s)" }
    }

    # ------------------------------------------------------------ build
    if (& $Run "build") {
        Step "cargo build --release -p server"
        if ((Invoke-Tool cargo @("build", "--release", "-p", "server")) -ne 0) { $Results["build"] = "FAIL"; $Failed = $true; throw "cargo build failed" }
    }
    if ((& $Run "build") -or (& $Run "validate") -or (& $Run "msi")) {
        if (-not (Test-Path $Exe)) { $Results["build"] = "FAIL"; $Failed = $true; throw "no $Exe; run the build stage" }
        $reported = Get-ToolOutput $Exe @("--version")
        if ($reported -like "*+fault-injection*") { $Results["build"] = "FAIL"; $Failed = $true; throw "$Exe reports '$reported': a fault-injection build must not be released" }
        if ($reported -ne "cameodb $Version") { $Results["build"] = "FAIL"; $Failed = $true; throw "$Exe reports '$reported', not 'cameodb $Version'; rebuild" }
        $BuiltHash = (Get-FileHash $Exe -Algorithm SHA256).Hash.ToLowerInvariant()
        if (& $Run "build") { Ok "$reported, sha256 $($BuiltHash.Substring(0, 16))..."; $Results["build"] = "PASS" }
    }

    # ------------------------------------------------------------ test
    if (& $Run "test") {
        Step "cargo test --workspace"
        if ((Invoke-Tool cargo @("test", "--workspace", "--no-fail-fast")) -eq 0) { $Results["test"] = "PASS" }
        else { $Results["test"] = "FAIL"; $TestsFailed = $true; Bad "cargo test --workspace failed; validate and msi still run" }
    }

    # ------------------------------------------------------------ validate
    if (& $Run "validate") {
        Step "scripts\validate\windows.ps1"
        $validate = Join-Path $RepoRoot "scripts\validate\windows.ps1"
        $arguments = @{ Bin = $Exe }
        if ($DockerImage) { $arguments.DockerImage = $DockerImage }
        & $validate @arguments
        if ($LASTEXITCODE -eq 0) { $Results["validate"] = "PASS" } else { $Results["validate"] = "FAIL"; $Failed = $true }
    }

    # ------------------------------------------------------------ msi
    if (& $Run "msi") {
        if ($Failed) { throw "not packaging: an earlier stage failed" }
        Step "scripts\build\build-msi.ps1"
        if ((Test-Path variable:BuiltHash) -and ((Get-FileHash $Exe -Algorithm SHA256).Hash.ToLowerInvariant() -ne $BuiltHash)) {
            $Results["msi"] = "FAIL"; $Failed = $true
            throw "$Exe changed after it was built and validated in this run; run the whole pass again"
        }
        $msiArguments = @{}
        if ($CertificateThumbprint) { $msiArguments.CertificateThumbprint = $CertificateThumbprint }
        & (Join-Path $RepoRoot "scripts\build\build-msi.ps1") @msiArguments
        if (-not $?) { $Results["msi"] = "FAIL"; $Failed = $true; throw "build-msi.ps1 failed" }
        $Results["msi"] = "PASS"
    }
}
catch {
    Write-Host ""
    Write-Host "stopped: $($_.Exception.Message)" -ForegroundColor Red
    $Failed = $true
}
finally {
    Write-Host ""
    Write-Host "========================================"
    Write-Host "CameoDB $Version  commit $Commit  $(Get-Date -Format 'yyyy-MM-dd HH:mm')"
    foreach ($name in @("preflight", "build", "test", "validate", "msi")) {
        if (-not ($Stage -contains $name)) { continue }
        $result = if ($Results.Contains($name)) { $Results[$name] } else { "not run" }
        $color = if ($result -eq "PASS") { "Green" } elseif ($result -eq "not run") { "Yellow" } else { "Red" }
        Write-Host ("  {0,-10} " -f $name) -NoNewline
        Write-Host $result -ForegroundColor $color
    }
    if (-not $Failed) {
        Write-Host ""
        Write-Host "Staged in $Dist"
        Get-ChildItem $Dist -File | ForEach-Object {
            Write-Host ("    {0,-36} {1,12:N0} bytes" -f $_.Name, $_.Length)
        }
        Write-Host "Copy that folder to dist\$Version\windows\ on the release machine, then run"
        Write-Host "scripts/release/release.sh --stage build there, which checks verify.json against the"
        Write-Host "files. Copy dist\$Version\windows-verify\ with it. Record the stage lines above and"
        Write-Host "the log ($LogFile) in RELEASE-CHECKLIST.md."
    }
    Stop-Transcript | Out-Null

    # The record scripts/release/build.sh checks on the release machine: which commit, which
    # files (by hash), which stages passed. Written for a failed run too, so a failure travels
    # with the files and cannot be mistaken for a pass. ASCII, which every reader accepts.
    function Get-Sha([string]$Path) {
        if (Test-Path $Path) { (Get-FileHash $Path -Algorithm SHA256).Hash.ToLowerInvariant() } else { $null }
    }
    $stagedExe = Join-Path $Dist "cameodb.exe"
    $stagedMsi = Join-Path $Dist "cameodb-$Version-x64.msi"
    $stages = [ordered]@{}
    foreach ($name in @("preflight", "build", "test", "validate", "msi")) {
        $stages[$name] = if ($Results.Contains($name)) { $Results[$name] } else { "not run" }
    }
    $record = [ordered]@{
        version      = $Version
        commit       = $FullCommit
        dirty        = $Dirty
        host         = $env:COMPUTERNAME
        finished_utc = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
        rustc        = (Get-ToolOutput rustc @("--version"))
        exe_sha256   = $(if (Test-Path $stagedExe) { Get-Sha $stagedExe } else { Get-Sha $Exe })
        msi_sha256   = (Get-Sha $stagedMsi)
        stages       = $stages
    }
    $record | ConvertTo-Json -Depth 4 | Set-Content -Path (Join-Path $LogDir "verify.json") -Encoding ascii
}
exit $(if ($Failed -or $TestsFailed) { 1 } else { 0 })
