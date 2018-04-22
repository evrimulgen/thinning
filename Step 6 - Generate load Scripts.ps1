[CmdletBinding()]

# MODIFY PARAMETERS THERE
$ConnectString = "THINNING/Qwertyu1@P-ORA11/ORCL"
$PathToCSV = "A:\Disk-F\Temp\4"           # without trailing slash (!)


$filenames = Get-ChildItem -name *.csv

$S = 0

Remove-Item *.ctl -ErrorAction SilentlyContinue
Remove-Item *.log -ErrorAction SilentlyContinue
Remove-Item *.bad -ErrorAction SilentlyContinue
Remove-Item *.dsc -ErrorAction SilentlyContinue
Remove-Item LoadData-Oracle.bat -ErrorAction SilentlyContinue
Remove-Item LoadData-MSSQL.sql -ErrorAction SilentlyContinue

ForEach ($FilenameExt in $Filenames)
{
	Write-Host "Processing file: "$FilenameExt
	$StockName = $FilenameExt.substring(1, $FilenameExt.Length-5)
	$FilenameCtl = '.'+$Stockname+'.ctl'
        Add-Content -Path $FilenameCtl -Value "OPTIONS (DIRECT=TRUE, PARALLEL=FALSE, ROWS=1000000, SKIP_INDEX_MAINTENANCE=Y)"
        Add-Content -Path $FilenameCtl -Value "UNRECOVERABLE"
        Add-Content -Path $FilenameCtl -Value "LOAD DATA"
        Add-Content -Path $FilenameCtl -Value "INFILE '.$StockName.csv'"
        Add-Content -Path $FilenameCtl -Value "BADFILE '.$StockName.bad'"
        Add-Content -Path $FilenameCtl -Value "DISCARDFILE '.$StockName.dsc'"
        Add-Content -Path $FilenameCtl -Value "INTO TABLE TRANSACTIONS_RAW"
        Add-Content -Path $FilenameCtl -Value "APPEND"
        Add-Content -Path $FilenameCtl -Value "FIELDS TERMINATED BY ','"
        Add-Content -Path $FilenameCtl -Value "(ID SEQUENCE ($S), STOCK_NAME constant '$StockName', UT, APRICE, AVOLUME)"
        Add-Content -Path LoadData-Oracle.bat -Value "sqlldr $ConnectString control=$FilenameCtl"


        Add-Content -Path LoadData-MSSQL.sql -Value "insert into TRANSACTIONS_RAW (STOCK_NAME, UT, APRICE, AVOLUME)"
        Add-Content -Path LoadData-MSSQL.sql -Value "select '$StockName' as STOCK_NAME, UT, APRICE, AVOLUME"
        Add-Content -Path LoadData-MSSQL.sql -Value "from openrowset (bulk '$PathToCSV\$FilenameExt', formatfile = '$PathToCSV\format_mssql.bcp') as T1;"
        Add-Content -Path LoadData-MSSQL.sql -Value ""

	$S = $S + 1000000000
}

