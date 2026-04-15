!include "LogicLib.nsh"

Var DeleteDbChoice
Var DeleteSecretsChoice

!define PERSIST_ROOT "$APPDATA\Clothes Hub\OzonatorPersistent"
!define PRESERVE_ROOT "$APPDATA\Clothes Hub\OzonatorPersistent\preserve"

Function un.PreservePersistentFile
  Exch $1
  Exch
  Exch $0
  IfFileExists "$0" 0 un_preserve_file_done
  CreateDirectory "${PRESERVE_ROOT}"
  CopyFiles /SILENT "$0" "$1"
un_preserve_file_done:
  Pop $0
  Pop $1
FunctionEnd

Function RestorePersistentFile
  Exch $1
  Exch
  Exch $0
  IfFileExists "$0" 0 restore_file_done
  CreateDirectory "$1"
  CopyFiles /SILENT "$0" "$1"
restore_file_done:
  Pop $0
  Pop $1
FunctionEnd

!macro customInit
  CreateDirectory "${PERSIST_ROOT}"
  Delete "${PERSIST_ROOT}\installer-ready.marker"

  StrCpy $0 "$INSTDIR\Озонатор.exe"
  IfFileExists "$0" +2 0
  StrCpy $0 "$LOCALAPPDATA\Programs\Озонатор\Озонатор.exe"
  IfFileExists "$0" 0 custom_init_done

  Exec '"$0" --installer-close-request'

  StrCpy $1 0
custom_init_wait_loop:
  IfFileExists "${PERSIST_ROOT}\installer-ready.marker" custom_init_ready
  Sleep 250
  IntOp $1 $1 + 1
  IntCmp $1 80 custom_init_timeout custom_init_wait_loop custom_init_wait_loop

custom_init_ready:
  Sleep 1200
  Delete "${PERSIST_ROOT}\installer-ready.marker"
  Goto custom_init_done

custom_init_timeout:
  ExecWait '"$SYSDIR\taskkill.exe" /IM "Озонатор.exe" /T'
  Sleep 800
  ExecWait '"$SYSDIR\taskkill.exe" /IM "Озонатор.exe" /T /F'
  Delete "${PERSIST_ROOT}\installer-ready.marker"

custom_init_done:
!macroend

!macro customInstall
  CreateDirectory "${PERSIST_ROOT}"
  CreateDirectory "$INSTDIR\data"

  Push "${PRESERVE_ROOT}\app.db"
  Push "$INSTDIR\data"
  Call RestorePersistentFile

  Push "${PRESERVE_ROOT}\secrets.json"
  Push "$INSTDIR\data"
  Call RestorePersistentFile

  Delete "${PRESERVE_ROOT}\app.db"
  Delete "${PRESERVE_ROOT}\secrets.json"
  RMDir "${PRESERVE_ROOT}"

  ClearErrors
  FileOpen $0 "${PERSIST_ROOT}\installer.marker" w
  IfErrors custom_install_done
  FileWrite $0 "1"
  FileClose $0
custom_install_done:
!macroend

!macro customUnInit
  StrCpy $DeleteDbChoice "0"
  StrCpy $DeleteSecretsChoice "0"

  MessageBox MB_YESNO|MB_ICONQUESTION|MB_DEFBUTTON2 "Удалить локальную базу Ozonator (app.db)?$\r$\n$\r$\nПо умолчанию база сохраняется." IDYES uninit_delete_db
  Goto uninit_after_db
uninit_delete_db:
  StrCpy $DeleteDbChoice "1"
uninit_after_db:

  MessageBox MB_YESNO|MB_ICONQUESTION|MB_DEFBUTTON2 "Удалить сохранённые Client-Id и Api-Key (secrets.json)?$\r$\n$\r$\nПо умолчанию ключи сохраняются." IDYES uninit_delete_secrets
  Goto uninit_done
uninit_delete_secrets:
  StrCpy $DeleteSecretsChoice "1"
uninit_done:
!macroend

!macro customUnInstall
  CreateDirectory "${PERSIST_ROOT}"
  CreateDirectory "${PRESERVE_ROOT}"

  ${If} $DeleteDbChoice == "0"
    Push "$INSTDIR\data\app.db"
    Push "${PRESERVE_ROOT}"
    Call un.PreservePersistentFile
  ${Else}
    Delete "${PRESERVE_ROOT}\app.db"
    Delete "$INSTDIR\data\app.db"
  ${EndIf}

  ${If} $DeleteSecretsChoice == "0"
    Push "$INSTDIR\data\secrets.json"
    Push "${PRESERVE_ROOT}"
    Call un.PreservePersistentFile
  ${Else}
    Delete "${PRESERVE_ROOT}\secrets.json"
    Delete "$INSTDIR\data\secrets.json"
  ${EndIf}

  ClearErrors
  FileOpen $0 "${PERSIST_ROOT}\uninstall.marker" w
  IfErrors custom_uninstall_done
  FileWrite $0 "1"
  FileClose $0
custom_uninstall_done:
!macroend
