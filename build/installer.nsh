!macro customInit
  CreateDirectory "$APPDATA\Clothes Hub\OzonatorPersistent"
  Delete "$APPDATA\Clothes Hub\OzonatorPersistent\installer-ready.marker"

  StrCpy $0 "$INSTDIR\Озонатор.exe"
  IfFileExists "$0" +2 0
  StrCpy $0 "$LOCALAPPDATA\Programs\Озонатор\Озонатор.exe"
  IfFileExists "$0" 0 custom_init_done

  Exec '"$0" --installer-close-request'

  StrCpy $1 0
custom_init_wait_loop:
  IfFileExists "$APPDATA\Clothes Hub\OzonatorPersistent\installer-ready.marker" custom_init_ready
  Sleep 250
  IntOp $1 $1 + 1
  IntCmp $1 80 custom_init_timeout custom_init_wait_loop custom_init_wait_loop

custom_init_ready:
  Sleep 1200
  Delete "$APPDATA\Clothes Hub\OzonatorPersistent\installer-ready.marker"
  Goto custom_init_done

custom_init_timeout:
  ExecWait '"$SYSDIR\taskkill.exe" /IM "Озонатор.exe" /T'
  Sleep 800
  ExecWait '"$SYSDIR\taskkill.exe" /IM "Озонатор.exe" /T /F'
  Delete "$APPDATA\Clothes Hub\OzonatorPersistent\installer-ready.marker"

custom_init_done:
!macroend

!macro customInstall
  CreateDirectory "$APPDATA\Clothes Hub\OzonatorPersistent"
  CreateDirectory "$INSTDIR\data"
  ClearErrors
  FileOpen $0 "$APPDATA\Clothes Hub\OzonatorPersistent\installer.marker" w
  IfErrors custom_install_done
  FileWrite $0 "1"
  FileClose $0
custom_install_done:
!macroend

!macro customUnInstall
  CreateDirectory "$APPDATA\Clothes Hub\OzonatorPersistent"
  ClearErrors
  FileOpen $0 "$APPDATA\Clothes Hub\OzonatorPersistent\uninstall.marker" w
  IfErrors custom_uninstall_done
  FileWrite $0 "1"
  FileClose $0
custom_uninstall_done:
!macroend
