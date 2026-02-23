!macro customInstall
  CreateDirectory "$APPDATA\Clothes Hub\OzonatorPersistent"
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
