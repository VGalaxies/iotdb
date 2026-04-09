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

pushd %~dp0..\..
if NOT DEFINED STREAMNODE_HOME set STREAMNODE_HOME=%cd%
popd

IF EXIST "%STREAMNODE_HOME%\conf\iotdb-streamnode.properties" (
  set config_file="%STREAMNODE_HOME%\conf\iotdb-streamnode.properties"
) ELSE (
  echo "No configuration file found. Exiting."
  exit /b 1
)

for /f  "eol=; tokens=2,2 delims==" %%i in ('findstr /i "^sn_internal_port"
"%config_file%"') do (
  set sn_internal_port=%%i
)
@REM trim the port
:delLeft1
if "%sn_internal_port:~0,1%"==" " (
    set "sn_internal_port=%sn_internal_port:~1%"
    goto delLeft1
)

:delRight1
if "%sn_internal_port:~-1%"==" " (
    set "sn_internal_port=%sn_internal_port:~0,-1%"
    goto delRight1
)

if not defined sn_internal_port (
  echo "WARNING: sn_internal_port not found in the configuration file. Using default value sn_internal_port = 10820"
  set sn_internal_port=10820
)

echo "check whether the sn_internal_port is used..., port is %sn_internal_port%"

for /f "tokens=5" %%a in ('netstat /ano ^| findstr :%sn_internal_port% ^| findstr LISTENING ') do (
  taskkill /f /pid %%a
    echo "close StreamNode, PID:" %%a
)
