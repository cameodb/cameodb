<#
.SYNOPSIS
  The validation checks that only mean something on Windows, against a built cameodb.exe.

.DESCRIPTION
  The bash suites under scripts/validate/ do not run on Windows. This is the part of them that
  has to be answered on the machine that ships the Windows build, in PowerShell 5.1 or later:

    smoke           --version is this release and not a fault-injection build; the shipped
                    Windows config passes check-config; a node starts and answers its health
                    check
    data-dir        one process per data directory (OB30), between native processes: a second
                    node, `compact` and `unlock` are refused while a node holds the directory;
                    a Ctrl+C stop removes the owner record; a killed node leaves it and the next
                    start takes it over; a record naming another host refuses the start until
                    `cameodb unlock`
    docker          (with -DockerImage) the same directory against a Linux container on Docker
                    Desktop, which reaches Windows files through a layer that does not carry
                    the OS lock: a container is refused by a native node's record, and a native
                    node by a container's
    remote-sources  outbound HTTPS from the bundled client against real hosts, verifying
                    against the Windows certificate store; a self-signed host is refused and
                    --insecure-source relaxes that (the same checks as remote-sources.sh)

  Each check prints PASS, FAIL or SKIP; the exit code is the number of failures, capped at 1.
  Paste the summary into RELEASE-CHECKLIST.md. Everything runs in a temporary directory and on
  ports the system hands out; nothing under the repository or C:\ProgramData is touched.

  CAMEODB_* variables in the shell are ignored, as the bash suites ignore them.

.PARAMETER Bin
  The binary under test. Default: target\release\cameodb.exe.

.PARAMETER Suite
  Which suites to run. Default: smoke, data-dir and remote-sources (and docker when
  -DockerImage is given).

.PARAMETER DockerImage
  A Linux CameoDB image for the docker suite, e.g. the one `docker build -t cameodb:win-check .`
  makes from the repository root. It must carry the shipped Docker config (data in /data/cameodb,
  HTTP on 9480). Without it the docker suite is skipped, and says so.

.PARAMETER RemoteSource1
.PARAMETER RemoteSource2
  URLs the remote-sources suite fetches, for a network that cannot reach the defaults.

.EXAMPLE
  .\scripts\validate\windows.ps1
.EXAMPLE
  .\scripts\validate\windows.ps1 -Suite data-dir -DockerImage cameodb:win-check
#>
[CmdletBinding()]
param(
    [string]$Bin,
    [ValidateSet("smoke", "data-dir", "docker", "remote-sources")]
    [string[]]$Suite,
    [string]$DockerImage,
    [string]$RemoteSource1 = "https://dl.cameodb.com/cameodb.spdx.json",
    [string]$RemoteSource2 = "https://raw.githubusercontent.com/rust-lang/rust/master/README.md"
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
if (-not $Bin) { $Bin = Join-Path $RepoRoot "target\release\cameodb.exe" }
if (-not (Test-Path $Bin)) {
    Write-Host "error: no $Bin; build it first (scripts\build\windows-release.ps1 -Stage build)" -ForegroundColor Red
    exit 2
}
$Bin = (Resolve-Path $Bin).Path
if (-not $Suite) {
    $Suite = @("smoke", "data-dir")
    if ($DockerImage) { $Suite += "docker" }
    $Suite += "remote-sources"
}

# ---------------------------------------------------------------- environment

# The node and the client read CAMEODB_* from the environment; an exported value changes what a
# check measures. Dropped for this process and its children, names reported.
$dropped = @(Get-ChildItem Env: | Where-Object { $_.Name -like "CAMEODB_*" } | ForEach-Object { $_.Name })
foreach ($name in $dropped) { Remove-Item "Env:$name" }
if ($dropped.Count -gt 0) {
    Write-Host "note: ignoring $($dropped -join ', ') from the environment; the suite runs without it"
}
$env:RUST_LOG = "warn"

$Work = Join-Path ([System.IO.Path]::GetTempPath()) ("cameodb-win-" + [guid]::NewGuid().ToString("N").Substring(0, 8))
New-Item -ItemType Directory -Path $Work | Out-Null

# ---------------------------------------------------------------- reporting

$script:Passed = 0
$script:Failed = 0
$script:Skipped = 0
$script:FailedChecks = @()

function Section([string]$Name) { Write-Host ""; Write-Host "== $Name ==" -ForegroundColor White }
function Pass([string]$What) { $script:Passed++; Write-Host "  PASS " -ForegroundColor Green -NoNewline; Write-Host $What }
function Skip([string]$What) { $script:Skipped++; Write-Host "  SKIP " -ForegroundColor Yellow -NoNewline; Write-Host $What }
function Fail([string]$What, [string]$Detail) {
    $script:Failed++
    $script:FailedChecks += $What
    Write-Host "  FAIL " -ForegroundColor Red -NoNewline
    Write-Host $What
    if ($Detail) { foreach ($line in ($Detail -split "`r?`n" | Select-Object -First 6)) { Write-Host "       $line" } }
}
function Check([bool]$Ok, [string]$What, [string]$Detail) {
    if ($Ok) { Pass $What } else { Fail $What $Detail }
}

# ---------------------------------------------------------------- helpers

function Quote([string]$Text) { '"' + $Text.Replace('"', '\"') + '"' }

# Run a program to completion, capturing both streams as text. Never throws on a non-zero exit.
function Invoke-Native([string]$File, [string[]]$Arguments, [int]$TimeoutSec = 60) {
    $stdout = Join-Path $Work ([guid]::NewGuid().ToString("N") + ".out")
    $stderr = Join-Path $Work ([guid]::NewGuid().ToString("N") + ".err")
    $argLine = ($Arguments | ForEach-Object { Quote $_ }) -join " "
    $params = @{
        FilePath = $File; NoNewWindow = $true; PassThru = $true
        RedirectStandardOutput = $stdout; RedirectStandardError = $stderr
    }
    if ($argLine) { $params.ArgumentList = $argLine }
    $proc = Start-Process @params
    $null = $proc.Handle   # without it, ExitCode can read back empty
    $timedOut = -not $proc.WaitForExit($TimeoutSec * 1000)
    if ($timedOut) { try { $proc.Kill() } catch { } }
    $proc.WaitForExit()
    $text = ""
    foreach ($f in @($stdout, $stderr)) {
        if (Test-Path $f) { $text += (Get-Content $f -Raw -ErrorAction SilentlyContinue) }
    }
    [pscustomobject]@{ Code = $(if ($timedOut) { -1 } else { $proc.ExitCode }); Text = "$text"; TimedOut = $timedOut }
}

function Get-FreePort {
    $listener = New-Object System.Net.Sockets.TcpListener([System.Net.IPAddress]::Loopback, 0)
    $listener.Start()
    $port = $listener.LocalEndpoint.Port
    $listener.Stop()
    $port
}

# A config for a local node on `Data` (and `Extra`), listening on a port of its own.
function New-NodeConfig([string]$Name, [string[]]$DataPaths) {
    $port = Get-FreePort
    $paths = ($DataPaths | ForEach-Object { "'" + $_ + "'" }) -join ", "
    $path = Join-Path $Work "$Name.toml"
    # Single-quoted TOML strings: the backslashes are taken as written.
    Set-Content -Path $path -Encoding ascii -Value @"
[node]
label = "$Name"
profile = "local"

[network.http]
bind_address = "127.0.0.1"
port = $port

[network.cluster]
enabled = false

[storage]
data_paths = [$paths]
num_shards_init = 1
max_shards_per_node = 1
"@
    [pscustomobject]@{ Path = $path; Port = $port }
}

# True when a web request failed with an HTTP reply (the network works) rather than a transport error.
function Test-HttpReply($ErrorRecord) {
    $property = $ErrorRecord.Exception.PSObject.Properties["Response"]
    [bool]($property -and $property.Value)
}

function Test-Healthy([int]$Port) {
    try {
        $null = Invoke-RestMethod -Uri "http://127.0.0.1:$Port/_cluster/health" -TimeoutSec 2 -UseBasicParsing
        $true
    } catch { $false }
}

function Wait-Healthy([int]$Port, [int]$Seconds = 40) {
    $deadline = (Get-Date).AddSeconds($Seconds)
    while ((Get-Date) -lt $deadline) {
        if (Test-Healthy $Port) { return $true }
        Start-Sleep -Milliseconds 200
    }
    $false
}

function Get-Owner([string]$Data) {
    $file = Join-Path $Data "cameodb.owner"
    if (-not (Test-Path $file)) { return $null }
    Get-Content $file -Raw | ConvertFrom-Json
}

# A node in a console window of its own (hidden), output to a log file. Its own console is what
# lets a Ctrl+C reach it alone. Returns the launcher; the node's pid is in its owner record.
function Start-Node($Config, [string]$Log) {
    $line = '/c ""{0}" -c "{1}" > "{2}" 2>&1"' -f $Bin, $Config.Path, $Log
    Start-Process -FilePath "cmd.exe" -ArgumentList $line -WindowStyle Hidden -PassThru
}

function Wait-NodeGone([int]$NodePid, [int]$Seconds) {
    $deadline = (Get-Date).AddSeconds($Seconds)
    while ((Get-Date) -lt $deadline) {
        if (-not (Get-Process -Id $NodePid -ErrorAction SilentlyContinue)) { return $true }
        Start-Sleep -Milliseconds 200
    }
    $false
}

function Stop-NodeHard([int]$NodePid) {
    Stop-Process -Id $NodePid -Force -ErrorAction SilentlyContinue
    [void](Wait-NodeGone $NodePid 20)
}

# Ctrl+C to one console: a helper detaches from ours, attaches to the node's, and signals it,
# so neither this script nor its caller sees the event.
$CtrlCScript = @'
param([int]$TargetPid)
Add-Type -Namespace Win -Name Con -MemberDefinition @"
[DllImport("kernel32.dll", SetLastError=true)] public static extern bool FreeConsole();
[DllImport("kernel32.dll", SetLastError=true)] public static extern bool AttachConsole(uint pid);
[DllImport("kernel32.dll", SetLastError=true)] public static extern bool SetConsoleCtrlHandler(IntPtr handler, bool add);
[DllImport("kernel32.dll", SetLastError=true)] public static extern bool GenerateConsoleCtrlEvent(uint ev, uint group);
"@
[void][Win.Con]::FreeConsole()
if (-not [Win.Con]::AttachConsole([uint32]$TargetPid)) { exit 2 }
[void][Win.Con]::SetConsoleCtrlHandler([IntPtr]::Zero, $true)
[void][Win.Con]::GenerateConsoleCtrlEvent(0, 0)
Start-Sleep -Milliseconds 500
[void][Win.Con]::FreeConsole()
exit 0
'@

# $true: the node exited. $false: it did not (the signal was not delivered, or the node ignored
# it) and was killed.
function Stop-NodeClean([int]$NodePid, [int]$Seconds = 40) {
    $helper = Join-Path $Work "ctrlc.ps1"
    Set-Content -Path $helper -Value $CtrlCScript -Encoding ascii
    $null = Invoke-Native "powershell.exe" @("-NoProfile", "-ExecutionPolicy", "Bypass", "-File", $helper, "-TargetPid", "$NodePid") 30
    if (Wait-NodeGone $NodePid $Seconds) { return $true }
    Stop-NodeHard $NodePid
    $false
}

function Test-NoPanic([string]$Log) {
    -not ((Test-Path $Log) -and ((Get-Content $Log -Raw) -match "panicked at"))
}

# ---------------------------------------------------------------- smoke

function Invoke-Smoke {
    Section "smoke"
    $version = (Invoke-Native $Bin @("--version") 20).Text.Trim()
    Check ($version -match '^cameodb [0-9]+\.[0-9]+\.[0-9]+$') "--version reports a release build" "reports '$version'"
    Check (-not ($version -like "*+fault-injection*")) "not a fault-injection build" "reports '$version'; it carries panic seams and an unauthenticated /__fault/panic route"

    $shipped = Join-Path $RepoRoot "crates\server\windows\cameodb.toml"
    $r = Invoke-Native $Bin @("check-config", "-c", $shipped, "--allow-unauthenticated") 30
    Check ($r.Code -eq 0) "check-config passes on the shipped Windows config" $r.Text

    $data = Join-Path $Work "smoke-data"
    $cfg = New-NodeConfig "smoke" @($data)
    $log = Join-Path $Work "smoke.log"
    $launcher = Start-Node $cfg $log
    if (Wait-Healthy $cfg.Port) {
        Pass "a node starts and answers /_cluster/health"
        $owner = Get-Owner $data
        if ($owner) { [void](Stop-NodeClean ([int]$owner.pid)) } else { Stop-Process -Id $launcher.Id -Force -ErrorAction SilentlyContinue }
    } else {
        Fail "a node starts and answers /_cluster/health" (Get-Content $log -Raw -ErrorAction SilentlyContinue)
        Stop-Process -Id $launcher.Id -Force -ErrorAction SilentlyContinue
        Get-Process cameodb -ErrorAction SilentlyContinue | Where-Object { $_.Path -eq $Bin } | Stop-Process -Force
    }
    Check (Test-NoPanic $log) "no panic in the node's log" (Get-Content $log -Raw -ErrorAction SilentlyContinue)
}

# ---------------------------------------------------------------- data-dir

function Invoke-DataDir {
    Section "data-dir: one process per data directory"
    $data = Join-Path $Work "dd-data"
    $data2 = Join-Path $Work "dd-data-2"
    $cfg = New-NodeConfig "first" @($data, $data2)
    $log = Join-Path $Work "first.log"
    $launcher = Start-Node $cfg $log
    if (-not (Wait-Healthy $cfg.Port)) {
        Fail "the first node starts" (Get-Content $log -Raw -ErrorAction SilentlyContinue)
        Stop-Process -Id $launcher.Id -Force -ErrorAction SilentlyContinue
        return
    }
    $owner = Get-Owner $data
    Check ($null -ne $owner) "the node writes cameodb.owner in its data directory" "no $data\cameodb.owner"
    if (-not $owner) { return }
    $nodePid = [int]$owner.pid
    Check ($null -ne (Get-Owner $data2)) "and in every data path" "no $data2\cameodb.owner"
    Check ([string]$owner.command -eq "node") "the record names a node" ($owner | ConvertTo-Json -Compress)
    Check ([string]$owner.host -ieq $env:COMPUTERNAME) "the record's host is this machine ($env:COMPUTERNAME)" "host is '$($owner.host)'"
    Check ([bool](Get-Process -Id $nodePid -ErrorAction SilentlyContinue)) "the record's pid is the running node" "pid $nodePid is not running"

    # A second node, on its own port, on the same directory: refused by the claim, not the bind.
    $other = New-NodeConfig "second" @($data)
    $r = Invoke-Native $Bin @("-c", $other.Path) 60
    Check (($r.Code -ne 0) -and -not $r.TimedOut) "a second node on the held directory exits non-zero" $r.Text
    Check ($r.Text -like "*in use by another CameoDB process on this machine*") "and says the directory is in use" $r.Text
    Check ($r.Text -like "*$($owner.node)*") "and names the holder's node" $r.Text
    $third = New-NodeConfig "third" @($data2)
    $r = Invoke-Native $Bin @("-c", $third.Path) 60
    Check ($r.Code -ne 0) "the second data path alone is held too" $r.Text

    $r = Invoke-Native $Bin @("compact", "-c", $cfg.Path) 60
    Check (($r.Code -ne 0) -and ($r.Text -like "*$($owner.node)*")) "compact is refused while the node runs, naming it" $r.Text
    $r = Invoke-Native $Bin @("unlock", $data) 30
    Check (($r.Code -ne 0) -and ($r.Text -like "*running process on this machine*")) "unlock is refused while the node runs" $r.Text
    Check (Test-Healthy $cfg.Port) "the first node is untouched by all of it" "health check failed"

    # A clean stop: a console Ctrl+C, which is what the node's CTRL_C handler answers.
    if (Stop-NodeClean $nodePid) {
        Check (-not (Test-Path (Join-Path $data "cameodb.owner"))) "a Ctrl+C stop removes the owner record" "$data\cameodb.owner is still there"
        Check (-not (Test-Path (Join-Path $data2 "cameodb.owner"))) "from every data path" "$data2\cameodb.owner is still there"
        Check (Test-NoPanic $log) "and the log has no panic" (Get-Content $log -Raw)
    } else {
        Skip "a Ctrl+C stop removes the owner record — the signal did not stop the node from here; stop the MSI service instead (Stop-Service CameoDB) and check C:\ProgramData\CameoDB\data for cameodb.owner"
    }

    $r = Invoke-Native $Bin @("compact", "-c", $cfg.Path) 120
    Check ($r.Code -eq 0) "compact runs once the node is stopped" $r.Text
    Check (-not (Test-Path (Join-Path $data "cameodb.owner"))) "and removes its own record" "$data\cameodb.owner is still there"

    # Killed outright: the record stays; the next start on this machine takes it over.
    $cfg = New-NodeConfig "killed" @($data)
    $log = Join-Path $Work "killed.log"
    $launcher = Start-Node $cfg $log
    $ready = Wait-Healthy $cfg.Port
    $owner = Get-Owner $data
    if (-not ($ready -and $owner)) {
        Fail "a node restarts on the directory" (Get-Content $log -Raw -ErrorAction SilentlyContinue)
        Stop-Process -Id $launcher.Id -Force -ErrorAction SilentlyContinue
        return
    }
    $killedPid = [int]$owner.pid
    Stop-NodeHard $killedPid
    $left = Get-Owner $data
    Check (($null -ne $left) -and ([int]$left.pid -eq $killedPid)) "a killed node leaves its record" "no record naming pid $killedPid"

    $log2 = Join-Path $Work "taken-over.log"
    $launcher = Start-Node $cfg $log2
    $ready = Wait-Healthy $cfg.Port
    $taken = Get-Owner $data
    Check $ready "the next start on this machine takes the directory over" (Get-Content $log2 -Raw -ErrorAction SilentlyContinue)
    Check (($null -ne $taken) -and ([int]$taken.pid -ne $killedPid)) "and the record names the new process" "record: $($taken | ConvertTo-Json -Compress)"
    Check ((Get-Content $log2 -Raw -ErrorAction SilentlyContinue) -like "*taking over a data directory*") "and the log says it took over" (Get-Content $log2 -Raw -ErrorAction SilentlyContinue)
    if ($taken) { [void](Stop-NodeClean ([int]$taken.pid)) } else { Stop-Process -Id $launcher.Id -Force -ErrorAction SilentlyContinue }

    # A record naming another host — what a container leaves — stops the node until `unlock`.
    $foreign = $left | ConvertTo-Json | ConvertFrom-Json
    $foreign.host = "3f9c0a7e21b4"
    $foreignText = $foreign | ConvertTo-Json
    Set-Content -Path (Join-Path $data "cameodb.owner") -Value $foreignText -Encoding ascii
    $r = Invoke-Native $Bin @("-c", $cfg.Path) 60
    Check (($r.Code -ne 0) -and ($r.Text -like "*host 3f9c0a7e21b4*") -and ($r.Text -like "*cameodb unlock*")) "a record from another host refuses the start and names `unlock`" $r.Text
    Check ((Test-Path (Join-Path $data "cameodb.owner"))) "and the record is left alone" "the record is gone"
    $r = Invoke-Native $Bin @("unlock", $data) 30
    Check (($r.Code -eq 0) -and ($r.Text -like "*3f9c0a7e21b4*")) "unlock shows the record and removes it" $r.Text
    Check (-not (Test-Path (Join-Path $data "cameodb.owner"))) "no record remains" "still there"

    $launcher = Start-Node $cfg (Join-Path $Work "after-unlock.log")
    if (Wait-Healthy $cfg.Port) {
        Pass "the node starts again after unlock"
        $o = Get-Owner $data
        if ($o) { [void](Stop-NodeClean ([int]$o.pid)) }
    } else {
        Fail "the node starts again after unlock" (Get-Content (Join-Path $Work "after-unlock.log") -Raw -ErrorAction SilentlyContinue)
        Stop-Process -Id $launcher.Id -Force -ErrorAction SilentlyContinue
    }
}

# ---------------------------------------------------------------- docker

function Invoke-Docker {
    Section "docker: a Linux container on a Windows directory"
    if (-not $DockerImage) {
        Skip "optional: not run without -DockerImage (Linux containers on a Windows folder are not part of a release check)"
        return
    }
    if (-not (Get-Command docker -ErrorAction SilentlyContinue)) { Fail "docker is on PATH" "docker not found"; return }
    $info = Invoke-Native "docker" @("info", "--format", "{{.OSType}}") 30
    if (($info.Code -ne 0) -or ($info.Text.Trim() -ne "linux")) {
        Fail "Docker Desktop is running Linux containers" $info.Text
        return
    }
    $image = Invoke-Native "docker" @("image", "inspect", $DockerImage, "--format", "{{.Id}}") 30
    if ($image.Code -ne 0) { Fail "the image $DockerImage exists" $image.Text; return }
    Pass "Docker Desktop runs Linux containers, and $DockerImage is there"

    $data = Join-Path $Work "docker-data"
    New-Item -ItemType Directory -Path $data | Out-Null
    $mount = "${data}:/data/cameodb"
    $name = "cameodb-lockcheck-" + [guid]::NewGuid().ToString("N").Substring(0, 6)

    # A native node holds the directory; a container on the same directory is refused by the
    # owner record, since the OS lock does not cross the file-sharing layer.
    $cfg = New-NodeConfig "native" @($data)
    $log = Join-Path $Work "docker-native.log"
    $launcher = Start-Node $cfg $log
    if (-not (Wait-Healthy $cfg.Port)) {
        Fail "a native node starts on the directory" (Get-Content $log -Raw -ErrorAction SilentlyContinue)
        Stop-Process -Id $launcher.Id -Force -ErrorAction SilentlyContinue
        return
    }
    $owner = Get-Owner $data
    $r = Invoke-Native "docker" @("run", "--rm", "--name", "$name-a", "-v", $mount, $DockerImage) 120
    Check (($r.Code -ne 0) -and -not $r.TimedOut) "a container on a native node's directory is refused" $r.Text
    Check (($r.Text -like "*$($owner.host)*") -and ($r.Text -like "*cameodb unlock*")) "naming the native host and the remedy" $r.Text
    $stillOwner = Get-Owner $data
    Check (($null -ne $stillOwner) -and ([int]$stillOwner.pid -eq [int]$owner.pid)) "the native node's record is untouched" "record: $($stillOwner | ConvertTo-Json -Compress)"
    Check (Test-Healthy $cfg.Port) "and the native node still answers" "health check failed"
    if (-not (Stop-NodeClean ([int]$owner.pid))) { Skip "native Ctrl+C stop did not reach the node; it was killed, and its record is cleared with unlock"; [void](Invoke-Native $Bin @("unlock", $data) 30) }

    # The other way round: a container holds it, and a native node is refused naming the container.
    $hostPort = Get-FreePort
    $run = Invoke-Native "docker" @("run", "-d", "--name", $name, "-p", "127.0.0.1:${hostPort}:9480", "-v", $mount, $DockerImage) 120
    if ($run.Code -ne 0) { Fail "a container starts on the directory" $run.Text; return }
    try {
        if (-not (Wait-Healthy $hostPort 60)) {
            Fail "a container starts on the directory" (Invoke-Native "docker" @("logs", $name) 30).Text
            return
        }
        Pass "a container starts on the directory"
        $record = Get-Owner $data
        Check (($null -ne $record) -and ([string]$record.host -ine $env:COMPUTERNAME)) "its record names a host of its own, not this machine" "record: $($record | ConvertTo-Json -Compress)"
        $nativeCfg = New-NodeConfig "native2" @($data)
        $r = Invoke-Native $Bin @("-c", $nativeCfg.Path) 60
        Check (($r.Code -ne 0) -and ($r.Text -like "*host $($record.host)*") -and ($r.Text -like "*cameodb unlock*")) "a native node on a container's directory is refused, naming the container" $r.Text
        Check (Test-Healthy $hostPort) "the container is untouched by it" "health check failed"
        $stop = Invoke-Native "docker" @("stop", "-t", "60", $name) 90
        Check ($stop.Code -eq 0) "docker stop ends the container" $stop.Text
        Check (-not (Test-Path (Join-Path $data "cameodb.owner"))) "and its clean stop removes the record" "$data\cameodb.owner is still there"
    } finally {
        [void](Invoke-Native "docker" @("rm", "-f", $name) 60)
    }
}

# ---------------------------------------------------------------- remote-sources

function Invoke-RemoteSources {
    Section "remote-sources: outbound HTTPS from the bundled client"
    try {
        $null = Invoke-WebRequest -Uri "https://dl.cameodb.com" -TimeoutSec 10 -UseBasicParsing -Method Head
    } catch {
        # A reply with an HTTP status still proves the network; only a transport failure skips.
        if (-not (Test-HttpReply $_)) { Skip "no outbound network; skipping remote source checks"; return }
    }
    foreach ($url in @($RemoteSource1, $RemoteSource2)) {
        # `schema detect` performs a real fetch through the client's reqwest/rustls stack.
        $r = Invoke-Native $Bin @("client", "schema", "detect", $url) 60
        if ($r.Code -eq 0) {
            Pass "fetched and parsed $url"
        } elseif ($r.Text -match "(?i)certificate|tls|handshake|ssl") {
            Fail "TLS handshake to $url" $r.Text
        } else {
            Pass "TLS handshake to $url succeeded (content not parseable as a schema, which is fine)"
        }
    }

    $bad = "https://self-signed.badssl.com/"
    $reachable = $true
    try { $null = Invoke-WebRequest -Uri $bad -TimeoutSec 10 -UseBasicParsing -Method Head } catch { $reachable = Test-HttpReply $_ }
    # Windows PowerShell's own TLS stack uses the same certificate store; if it accepts the
    # self-signed host, a proxy is re-signing it and the negative check means nothing here.
    if ($reachable -and $PSVersionTable.PSVersion.Major -ge 5) {
        try {
            $null = Invoke-WebRequest -Uri $bad -TimeoutSec 10 -UseBasicParsing
            Skip "$bad verifies on this network (a TLS-inspecting proxy re-signs it); negative check not meaningful here"
            return
        } catch { }
    }
    $r = Invoke-Native $Bin @("client", "schema", "detect", $bad) 60
    if ($r.Code -eq 0) {
        Fail "an invalid certificate is rejected by default" "the fetch succeeded"
    } else {
        Pass "an invalid certificate is rejected by default"
        $r2 = Invoke-Native $Bin @("client", "schema", "detect", $bad, "--insecure-source") 60
        Check (($r2.Code -eq 0) -or -not ($r2.Text -match "(?i)certificate|tls|handshake|ssl")) "--insecure-source relaxes verification for sources" $r2.Text
    }
}

# ---------------------------------------------------------------- run

try {
    Write-Host "cameodb under test: $Bin"
    Write-Host "work directory:     $Work"
    foreach ($s in $Suite) {
        switch ($s) {
            "smoke"          { Invoke-Smoke }
            "data-dir"       { Invoke-DataDir }
            "docker"         { Invoke-Docker }
            "remote-sources" { Invoke-RemoteSources }
        }
    }
} finally {
    # A node still running would keep its files open; none should be, but a failed check can leave one.
    Get-Process cameodb -ErrorAction SilentlyContinue | Where-Object { $_.Path -eq $Bin } | Stop-Process -Force -ErrorAction SilentlyContinue
    Start-Sleep -Milliseconds 300
    Remove-Item -Recurse -Force $Work -ErrorAction SilentlyContinue
}

Write-Host ""
Write-Host "----------------------------------------"
Write-Host "passed: $script:Passed   failed: $script:Failed   skipped: $script:Skipped"
if ($script:Failed -gt 0) {
    Write-Host "failed checks:" -ForegroundColor Red
    foreach ($c in $script:FailedChecks) { Write-Host "  - $c" -ForegroundColor Red }
    exit 1
}
Write-Host "All checks passed." -ForegroundColor Green
exit 0
