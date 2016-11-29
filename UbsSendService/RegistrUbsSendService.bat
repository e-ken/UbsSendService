rem 1 -Путь к рабочему каталогу системы
rem 2 -Пользователь MTS
rem 3 - его пароль (если EMPTY_PASSWORD, то без пароля)
rem 4 - имя сервера приложений

if "%3"=="EMPTY_PASSWORD" goto EP
	%1UbsSendService.exe /i %2 %3
	goto END1
	
:EP
%1UbsSendService.exe /i %2

END1:
rem Конец

