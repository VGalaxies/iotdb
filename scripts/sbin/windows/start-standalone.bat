@REM
@REM Licensed to the Apache Software Foundation (ASF) under one
@REM or more contributor license agreements.  See the NOTICE file
@REM distributed with this work for additional information
@REM regarding copyright ownership.  The ASF licenses this file
@REM to you under the Apache License, Version 2.0 (the
@REM "License"); you may not use this file except in compliance
@REM with the License.  You may obtain a copy of the License at
@REM
@REM     http://www.apache.org/licenses/LICENSE-2.0
@REM
@REM Unless required by applicable law or agreed to in writing,
@REM software distributed under the License is distributed on an
@REM "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
@REM KIND, either express or implied.  See the License for the
@REM specific language governing permissions and limitations
@REM under the License.
@REM

@echo off
setlocal ENABLEDELAYEDEXPANSION

pushd %~dp0..\..
if NOT DEFINED IOTDB_HOME set IOTDB_HOME=%cd%
popd

IF EXIST "%IOTDB_HOME%\sbin\windows\start-confignode.bat" (
  SET CONFIGNODE_START_PATH="%IOTDB_HOME%\sbin\windows\start-confignode.bat"
) ELSE (
  echo "Can't find start-confignode.bat."
  exit 0
)

IF EXIST "%IOTDB_HOME%\sbin\windows\start-datanode.bat" (
  SET DATANODE_START_PATH="%IOTDB_HOME%\sbin\windows\start-datanode.bat"
) ELSE (
  echo "Can't find start-datanode.bat."
  exit 0
)

REM Parse --with_streamnode
set with_streamnode=0
for %%i in (%*) do (
  if "%%i"=="--with_streamnode" set with_streamnode=1
)

if !with_streamnode!==1 (
  if not defined MEMORY_SIZE (
    for /f %%b in ('powershell -NoProfile -Command "$v=$host.Version.Major; $mem=if($v -lt 3){(Get-WmiObject Win32_ComputerSystem).TotalPhysicalMemory} else{(Get-CimInstance -ClassName Win32_ComputerSystem).TotalPhysicalMemory}; [math]::Round($mem/1048576)"') do set system_mb=%%b
    set /a cn_mb=!system_mb! / 10 * 2
    set /a dn_mb=!system_mb! / 10 * 4
    set MEMORY_SIZE=!cn_mb!M
    start cmd /c %CONFIGNODE_START_PATH%

    if defined IOTDB_NO_PAUSE (
      @REM TIMEOUT fails immediately when CI redirects stdin, so use a non-interactive delay.
      ping 127.0.0.1 -n 6 >NUL
    ) ELSE (
      TIMEOUT /T 5 /NOBREAK
    )

    set MEMORY_SIZE=!dn_mb!M
    start cmd /c %DATANODE_START_PATH%
  ) else (
    start cmd /c %CONFIGNODE_START_PATH%

    if defined IOTDB_NO_PAUSE (
      @REM TIMEOUT fails immediately when CI redirects stdin, so use a non-interactive delay.
      ping 127.0.0.1 -n 6 >NUL
    ) ELSE (
      TIMEOUT /T 5 /NOBREAK
    )

    start cmd /c %DATANODE_START_PATH%
  )
) else (
  start cmd /c %CONFIGNODE_START_PATH%

  if defined IOTDB_NO_PAUSE (
    @REM TIMEOUT fails immediately when CI redirects stdin, so use a non-interactive delay.
    ping 127.0.0.1 -n 6 >NUL
  ) ELSE (
    TIMEOUT /T 5 /NOBREAK
  )

  start cmd /c %DATANODE_START_PATH%
)

echo "Execute start-standalone.bat finished, you can see more details in the logs of confignode and datanode"
endlocal
exit 0