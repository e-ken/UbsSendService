rem 1 -���� � �������� �������� �������
rem 2 -������������ MTS
rem 3 - ��� ������ (���� EMPTY_PASSWORD, �� ��� ������)
rem 4 - ��� ������� ����������

if "%3"=="EMPTY_PASSWORD" goto EP
	%1UbsSendService.exe /i %2 %3
	goto END1
	
:EP
%1UbsSendService.exe /i %2

END1:
rem �����

