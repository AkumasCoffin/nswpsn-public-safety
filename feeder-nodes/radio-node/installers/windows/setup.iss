; ============================================================================
;  NSW PSN radio feeder node - Windows installer (Inno Setup 6)
; ----------------------------------------------------------------------------
;  Builds NSWPSN-Node-Setup.exe. The site renames the download to
;  NSWPSN-Node-Setup_<npsn_token>.exe; this installer PARSES ITS OWN FILENAME
;  at runtime to recover the node token, so the volunteer just double-clicks it.
;
;  If the filename carries no token (e.g. renamed by the browser), a wizard page
;  prompts the volunteer to paste one, plus an optional server URL.
;
;  Compile with:  iscc setup.iss              (Inno Setup 6, ANSI or Unicode)
;  Requires nodeagent.exe to sit next to this script (see #define below).
; ============================================================================

#define AppName        "NSWPSN Node"
#define AppVersion     "0.1.0"
#define AppPublisher   "Forcequit"
#define AppURL         "https://nswpsn.forcequit.xyz"
#define DefaultServer  "https://api.forcequit.xyz"
#define ZadigURL       "https://zadig.akeo.ie/"

; Path to the compiled Go agent binary, relative to this .iss. Override on the
; command line with:  iscc /DAgentExe="C:\path\to\nodeagent.exe" setup.iss
#ifndef AgentExe
  #define AgentExe "nodeagent.exe"
#endif

[Setup]
AppId={{9E1B2C7A-3F44-4A6E-9C11-7A2D5E0B4C10}}
AppName={#AppName}
AppVersion={#AppVersion}
AppPublisher={#AppPublisher}
AppPublisherURL={#AppURL}
AppSupportURL={#AppURL}
DefaultDirName={autopf}\{#AppName}
DefaultGroupName={#AppName}
DisableProgramGroupPage=yes
DisableDirPage=no
UninstallDisplayName={#AppName}
UninstallDisplayIcon={app}\nodeagent.exe
OutputBaseFilename=NSWPSN-Node-Setup
Compression=lzma2
SolidCompression=yes
WizardStyle=modern
ArchitecturesInstallIn64BitMode=x64compatible
; Service registration + writes under {commonappdata} both need elevation.
PrivilegesRequired=admin

[Languages]
Name: "english"; MessagesFile: "compiler:Default.isl"

[Files]
Source: "{#AgentExe}"; DestDir: "{app}"; DestName: "nodeagent.exe"; Flags: ignoreversion

[Icons]
Name: "{group}\{#AppName} (folder)"; Filename: "{app}"
Name: "{group}\Uninstall {#AppName}"; Filename: "{uninstallexe}"

[Run]
; Optional: open Zadig so the volunteer can flash the WinUSB driver onto their
; RTL-SDR. Unchecked by default; the finish page also explains this.
Filename: "{#ZadigURL}"; Description: "Open Zadig to install the RTL-SDR WinUSB driver (recommended)"; \
  Flags: postinstall shellexec nowait unchecked

[UninstallRun]
; Deregister + stop the Windows service before files are removed.
Filename: "{app}\nodeagent.exe"; Parameters: "uninstall"; Flags: runhidden; RunOnceId: "UnregisterNodeAgentService"

[Code]
const
  ProgDataSub = 'NSWPSN Node';

var
  TokenPage: TInputQueryPage;
  DetectedToken: string;

{ ---- token parsing helpers ---------------------------------------------- }

function IsHexChar(C: Char): Boolean;
begin
  Result := ((C >= '0') and (C <= '9')) or
            ((C >= 'a') and (C <= 'f')) or
            ((C >= 'A') and (C <= 'F'));
end;

{ Scan S for the first "npsn_" followed by exactly 40 hex chars and return the
  whole "npsn_<40 hex>" token (lower-cased). Returns '' if none is present. }
function FindToken(const S: string): string;
var
  I, K, N: Integer;
  Ok: Boolean;
begin
  Result := '';
  N := Length(S);
  I := 1;
  while I <= N - 4 do
  begin
    if LowerCase(Copy(S, I, 5)) = 'npsn_' then
    begin
      if I + 5 + 40 - 1 <= N then
      begin
        Ok := True;
        for K := 0 to 39 do
          if not IsHexChar(S[I + 5 + K]) then
          begin
            Ok := False;
            Break;
          end;
        if Ok then
        begin
          Result := LowerCase(Copy(S, I, 5 + 40));
          Exit;
        end;
      end;
    end;
    Inc(I);
  end;
end;

{ The basename of the running setup executable, e.g.
  "NSWPSN-Node-Setup_npsn_abc...def.exe". }
function SetupBaseName(): string;
begin
  Result := ExtractFileName(ExpandConstant('{srcexe}'));
end;

{ ---- wizard ------------------------------------------------------------- }

procedure InitializeWizard();
begin
  DetectedToken := FindToken(SetupBaseName());

  { A single page with two fields: node token (required) + server URL (optional).
    Shown only when the filename carried no token (see ShouldSkipPage). }
  TokenPage := CreateInputQueryPage(wpWelcome,
    'Node key',
    'Enter the feeder node key from your download page',
    'This installer normally reads your key automatically from its filename. ' +
    'Since it could not, paste the key shown on the feeder page below. ' +
    'It looks like "npsn_" followed by 40 characters.');
  TokenPage.Add('Node key (npsn_...):', False);
  TokenPage.Add('Server URL (leave as-is unless told otherwise):', False);
  TokenPage.Values[0] := '';
  TokenPage.Values[1] := '{#DefaultServer}';
end;

function ShouldSkipPage(PageID: Integer): Boolean;
begin
  Result := False;
  { Skip the manual-entry page when we already recovered a token. }
  if (PageID = TokenPage.ID) and (DetectedToken <> '') then
    Result := True;
end;

function NextButtonClick(CurPageID: Integer): Boolean;
var
  Entered: string;
begin
  Result := True;
  if CurPageID = TokenPage.ID then
  begin
    Entered := FindToken(Trim(TokenPage.Values[0]));
    if Entered = '' then
    begin
      MsgBox('That does not look like a valid node key. It must be "npsn_" ' +
             'followed by 40 hex characters (0-9, a-f).', mbError, MB_OK);
      Result := False;
    end;
  end;
end;

{ Resolve the final token: filename first, else the manual-entry field. }
function ResolveToken(): string;
begin
  if DetectedToken <> '' then
    Result := DetectedToken
  else
    Result := FindToken(Trim(TokenPage.Values[0]));
end;

function ResolveServer(): string;
begin
  Result := Trim(TokenPage.Values[1]);
  if Result = '' then
    Result := '{#DefaultServer}';
end;

{ ---- agent.yaml (in %ProgramData%\NSWPSN Node) --------------------------- }

function ProgDataDir(): string;
begin
  Result := ExpandConstant('{commonappdata}\' + ProgDataSub);
end;

function ConfigPath(): string;
begin
  Result := ProgDataDir() + '\agent.yaml';
end;

{ Read an existing install_id so re-installs / upgrades keep the node's stable
  identity. Returns '' if there is no prior config or no install_id line. }
function ReadExistingInstallID(): string;
var
  Lines: TArrayOfString;
  I, P: Integer;
  L, V: string;
begin
  Result := '';
  if not FileExists(ConfigPath()) then
    Exit;
  if not LoadStringsFromFile(ConfigPath(), Lines) then
    Exit;
  for I := 0 to GetArrayLength(Lines) - 1 do
  begin
    L := Trim(Lines[I]);
    if (Length(L) >= 11) and (LowerCase(Copy(L, 1, 11)) = 'install_id:') then
    begin
      V := Trim(Copy(L, 12, Length(L)));
      { strip surrounding quotes if present }
      StringChangeEx(V, '"', '', True);
      StringChangeEx(V, '''', '', True);
      Result := Trim(V);
      Exit;
    end;
  end;
end;

{ Forward-slash form of a Windows path so it is safe inside a YAML double-quoted
  scalar (backslashes are escape chars in YAML "..."). Go accepts / on Windows. }
function ToYamlPath(const P: string): string;
begin
  Result := P;
  StringChangeEx(Result, '\', '/', True);
end;

{ Lock down a path so only SYSTEM (the service account) + Administrators can read
  it. C:\ProgramData grants BUILTIN\Users read by default and children inherit it,
  which would leave node_token / keys.json / rdio-admin.secret readable by any
  local user. /inheritance:r strips the inherited Users ACE; the (OI)(CI) grants
  apply to files the agent creates later too. SIDs are locale-independent:
  S-1-5-18 = LocalSystem, S-1-5-32-544 = Administrators. }
procedure HardenAcl(Path: string);
var
  ResultCode: Integer;
begin
  Exec(ExpandConstant('{sys}\icacls.exe'),
       '"' + Path + '" /inheritance:r /grant:r "*S-1-5-18:(OI)(CI)F" "*S-1-5-32-544:(OI)(CI)F" /T /C /Q',
       '', SW_HIDE, ewWaitUntilTerminated, ResultCode);
end;

procedure WriteAgentYaml();
var
  Cfg: TStringList;
  Token, Server, InstallID, DataDir: string;
begin
  ForceDirectories(ProgDataDir());
  { Harden BEFORE writing secrets so the file is never briefly world-readable. }
  HardenAcl(ProgDataDir());

  Token     := ResolveToken();
  Server    := ResolveServer();
  InstallID := ReadExistingInstallID();  { '' -> agent generates one on first run }
  DataDir   := ToYamlPath(ProgDataDir() + '\data');

  Cfg := TStringList.Create;
  try
    Cfg.Add('# NSW PSN radio feeder node - generated by the Windows installer.');
    Cfg.Add('# Do not share this file: node_token authenticates this node.');
    Cfg.Add('');
    Cfg.Add('server_url: "' + Server + '"');
    Cfg.Add('ws_url: ""');
    Cfg.Add('node_token: "' + Token + '"');
    Cfg.Add('install_id: "' + InstallID + '"');
    Cfg.Add('data_dir: "' + DataDir + '"');
    Cfg.Add('relay_addr: "127.0.0.1:17390"');
    Cfg.Add('sdrtrunk_control_port: 17392');
    Cfg.Add('');
    Cfg.Add('# SDR-Trunk + rdio-scanner runtimes are downloaded by the agent on');
    Cfg.Add('# first run; it fills in the command paths below automatically.');
    Cfg.Add('sdrtrunk:');
    Cfg.Add('  enabled: true');
    Cfg.Add('  command: ""');
    Cfg.Add('  args: []');
    Cfg.Add('rdio:');
    Cfg.Add('  enabled: true');
    Cfg.Add('  command: ""');
    Cfg.Add('  args: []');
    Cfg.SaveToFile(ConfigPath());
  finally
    Cfg.Free;
  end;
  { Re-apply to the file explicitly (it was just created under the dir). }
  HardenAcl(ConfigPath());
end;

{ ---- service registration ----------------------------------------------- }

procedure RegisterService();
var
  ResultCode: Integer;
begin
  { nodeagent install registers + starts the Windows service. Pass --config so
    the service reads the config we just wrote under ProgramData. }
  if not Exec(ExpandConstant('{app}\nodeagent.exe'), 'install --config "' + ConfigPath() + '"',
              '', SW_HIDE, ewWaitUntilTerminated, ResultCode) then
    MsgBox('Could not launch nodeagent.exe to register the service.' + #13#10 +
           'You can register it later from an elevated prompt:' + #13#10 +
           '  "' + ExpandConstant('{app}\nodeagent.exe') + '" install',
           mbError, MB_OK)
  else if ResultCode <> 0 then
    MsgBox('The feeder service did not register cleanly (exit code ' +
           IntToStr(ResultCode) + ').' + #13#10 +
           'Check the agent logs under "' + ProgDataDir() + '\data\logs".',
           mbInformation, MB_OK);
end;

procedure CurStepChanged(CurStep: TSetupStep);
begin
  if CurStep = ssPostInstall then
  begin
    WriteAgentYaml();
    RegisterService();
  end;
end;

procedure CurPageChanged(CurPageID: Integer);
begin
  if CurPageID = wpFinished then
    WizardForm.FinishedLabel.Caption :=
      'Your NSW PSN feeder node is installed and running as a background service.' + #13#10 + #13#10 +
      'If you use an RTL-SDR dongle, it needs the WinUSB driver: run Zadig ' +
      '(' + '{#ZadigURL}' + '), select your dongle, and install "WinUSB". ' +
      'You can tick the box below to open Zadig now.' + #13#10 + #13#10 +
      'Node status appears back on the feeder page on the website.';
end;
