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
@REM set cmd format
powershell -NoProfile -Command "$v=(Get-ItemProperty 'HKLM:\SOFTWARE\Microsoft\Windows NT\CurrentVersion').CurrentMajorVersionNumber; if($v -gt 6) { cmd /c 'chcp 65001' }"

title IoTDB StreamNode

echo ```````````````````````````
echo Starting IoTDB StreamNode
echo ```````````````````````````

set PATH="%JAVA_HOME%\bin\";%PATH%
set "FULL_VERSION="
set "MAJOR_VERSION="
set "MINOR_VERSION="


for /f tokens^=2-5^ delims^=.-_+^" %%j in ('java -fullversion 2^>^&1') do (
	set "FULL_VERSION=%%j-%%k-%%l-%%m"
	IF "%%j" == "1" (
	    set "MAJOR_VERSION=%%k"
	    set "MINOR_VERSION=%%l"
	) else (
	    set "MAJOR_VERSION=%%j"
	    set "MINOR_VERSION=%%k"
	)
)

set JAVA_VERSION=%MAJOR_VERSION%

@REM we do not check jdk that version less than 1.8 because they are too stale...
IF "%JAVA_VERSION%" == "6" (
	echo IoTDB only supports jdk >= 8, please check your java version.
	goto finally
)
IF "%JAVA_VERSION%" == "7" (
	echo IoTDB only supports jdk >= 8, please check your java version.
	goto finally
)

if "%OS%" == "Windows_NT" setlocal

pushd %~dp0..\..
if NOT DEFINED STREAMNODE_HOME set STREAMNODE_HOME=%cd%
popd

set STREAMNODE_CONF=%STREAMNODE_HOME%\conf
set STREAMNODE_LOGS=%STREAMNODE_HOME%\logs

@setlocal ENABLEDELAYEDEXPANSION ENABLEEXTENSIONS
set is_conf_path=false
for %%i in (%*) do (
	IF "%%i" == "-c" (
		set is_conf_path=true
	) ELSE IF "!is_conf_path!" == "true" (
		set is_conf_path=false
		set STREAMNODE_CONF=%%i
	)
)

IF EXIST "%STREAMNODE_CONF%\windows\streamnode-env.bat" (
    CALL "%STREAMNODE_CONF%\windows\streamnode-env.bat" %1
    ) ELSE (
    echo "Can't find %STREAMNODE_CONF%\windows\streamnode-env.bat"
    )

@REM CHECK THE PORT USAGES
@REM SET CONFIG FILE
IF EXIST "%STREAMNODE_CONF%\iotdb-streamnode.properties" (
  set CONFIG_FILE="%STREAMNODE_CONF%\iotdb-streamnode.properties"
) ELSE IF EXIST "%STREAMNODE_HOME%\conf\iotdb-streamnode.properties" (
  set CONFIG_FILE="%STREAMNODE_HOME%\conf\iotdb-streamnode.properties"
) ELSE (
  set CONFIG_FILE=
)

IF DEFINED CONFIG_FILE (
  for /f  "eol=# tokens=2 delims==" %%i in ('findstr /i "^sn_internal_port"
    "%CONFIG_FILE%"') do (
      set sn_internal_port=%%i
  )
) ELSE (
  echo "Can't find iotdb-streamnode.properties, check the default ports"
  set sn_internal_port=10820
)

set CONF_PARAMS=-s
if NOT DEFINED MAIN_CLASS set MAIN_CLASS=org.apache.iotdb.streamnode.StreamNode
if NOT DEFINED JAVA_HOME goto :err

@REM -----------------------------------------------------------------------------
@REM JVM Opts we'll use in legacy run or installation
set JAVA_OPTS=-ea^
 -Dlogback.configurationFile="%STREAMNODE_CONF%\logback-streamnode.xml"^
 -DSTREAMNODE_HOME="%STREAMNODE_HOME%"^
 -DSTREAMNODE_CONF="%STREAMNODE_CONF%"^
 -Dsun.jnu.encoding=UTF-8^
 -Dfile.encoding=UTF-8

@REM ***** CLASSPATH library setting *****
@REM Ensure that any user defined CLASSPATH variables are not used on startup
if EXIST "%STREAMNODE_HOME%\lib" (set CLASSPATH="%STREAMNODE_HOME%\lib\*") else set CLASSPATH="%STREAMNODE_HOME%\..\lib\*"
set CLASSPATH=%CLASSPATH%;iotdb.StreamNode
goto okClasspath

:append
set CLASSPATH=%CLASSPATH%;%1

goto :eof

@REM -----------------------------------------------------------------------------
:okClasspath

rem echo CLASSPATH: %CLASSPATH%

@REM SET PARA

@REM StreamNode runs in foreground by default
set foreground=yes

:checkPara
set COMMANSLINE=%*
@REM setlocal ENABLEDELAYEDEXPANSION
:STR_VISTOR
for /f "tokens=1* delims= " %%a in ("%COMMANSLINE%") do (
@REM -----more para-----
for /f "tokens=1* delims==" %%1 in ("%%a") do (
if "%%1"=="-f" ( set foreground=yes)
if "%%1"=="-d" ( set foreground=0)
)
set COMMANSLINE=%%b
goto STR_VISTOR
)

@REM ----------------------------------------------------------------------------
@REM START
:start
if %foreground%==yes (
	java %ILLEGAL_ACCESS_PARAMS% %JAVA_OPTS% %STREAMNODE_HEAP_OPTS% -cp %CLASSPATH% %STREAMNODE_JMX_OPTS% %MAIN_CLASS% %CONF_PARAMS%
	) ELSE (
	start javaw %ILLEGAL_ACCESS_PARAMS% %JAVA_OPTS% %STREAMNODE_HEAP_OPTS% -cp %CLASSPATH% %STREAMNODE_JMX_OPTS% %MAIN_CLASS% %CONF_PARAMS%
	)
goto finally

:err
echo JAVA_HOME environment variable must be set!
pause

:finally
@ENDLOCAL
pause
