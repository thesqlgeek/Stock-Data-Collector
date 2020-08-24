
#Parameter Variables
$dataSource = ".\SQL2017"
$rootfiledir = "D:\StockMarketData\"


#Define Variable Set
$database = "GlobalStockMarketDB"
$sqlbulkloadtable = "Data.StockHistoricalBulkLoad"
$proc_start_time = Get-Date
$oktoTruncateSQLTable = $True
$connectionString = "Server=$dataSource;Database=$database;Integrated Security=True;"
$stockdldfilepath = $rootfiledir + "Stock Files 20200819\"
$batchsize = 100000
$elapsed = [System.Diagnostics.Stopwatch]::StartNew()
$firstRowColumnNames = $true
$csvdelimiter = "," #"`t"
	
#Create Logs

$stockdldlogfilepath = $rootfiledir + "Log Files\"
$InitLogFileStart = "Stock Data Extract started at " + $proc_start_time

$RunLogOutputFile = $stockdldlogfilepath + "Stock Data Extract Log " + ($proc_start_time -replace ('[#?&\/{:]', ''))
$RunLogOutputFile = $RunLogOutputFile + ".txt"

$ErrorLogOutputFile = $stockdldlogfilepath + "Stock Data Error Log " + ($proc_start_time -replace ('[#?&\/{:]', ''))
$ErrorLogOutputFile = $ErrorLogOutputFile + ".txt"

#Verify and Create Directory and Paths for Log Files and Download Files Holding

if ((Test-Path $stockdldfilepath) -eq 0) { New-Item -ItemType Directory -Force -Path $stockdldfilepath}
if ((Test-Path $stockdldlogfilepath) -eq 0) { New-Item -ItemType Directory -Force -Path $stockdldlogfilepath }

#Init Log Files

$InitLogFileStart | Out-File -FilePath "$RunLogOutputFile" -Append
'--------------------------------------------------------------------------------------------------------' | Out-File -FilePath "$RunLogOutputFile" -Append

$InitLogFileStart | Out-File -FilePath "$ErrorLogOutputFile" -Append
'--------------------------------------------------------------------------------------------------------' | Out-File -FilePath "$ErrorLogOutputFile" -Append


#Create the Datatable to be used for the Stream storage and for the Bulk Load

$datatable = New-Object System.Data.DataTable  

$null = $datatable.Columns.Add('StockID', ([int32]))
$null = $datatable.Columns.Add('StockHistDate', ([String]))
$null = $datatable.Columns.Add('StockHistOpen', ([Decimal]))
$null = $datatable.Columns.Add('StockHistHigh', ([Decimal]))
$null = $datatable.Columns.Add('StockHistLow', ([Decimal]))
$null = $datatable.Columns.Add('StockHistClosed', ([Decimal]))
$null = $datatable.Columns.Add('StockHistAdjustedClose', ([Decimal]))
$null = $datatable.Columns.Add('StockHistVolume', ([int32]))
$null = $datatable.Columns.Add('StockHistDateAdded', ([DateTime]))

#Create the Bulkloader strings

$bulkcopy = New-Object Data.SqlClient.SqlBulkCopy($connectionstring, [System.Data.SqlClient.SqlBulkCopyOptions]::TableLock)
$bulkcopy.DestinationTableName = $sqlbulkloadtable
$bulkcopy.bulkcopyTimeout = 0
$bulkcopy.batchsize = $batchsize

#Create Connection for SQL to Retrive Symbols for working set and retrieve the working set

$connection = New-Object System.Data.SqlClient.SqlConnection
$connection.ConnectionString = $connectionString

$connection.Open()

if ($oktoTruncateSQLTable -eq $true)
{
	
	$truncquery = "Truncate table $database.$sqlbulkloadtable"
	
	"Received OK to Truncate. Issued command - $truncquery" | Out-File -FilePath "$RunLogOutputFile" -Append #Log the Query 
	
	$command = $connection.CreateCommand()
	$command.CommandText = $truncquery
	$command.ExecuteNonQuery()
}


#$query = "Select stockID, StockTicker from [Ref].[Stock] where StockCountry = 'USA' and IsStockActive = 1 and StockID = '14653' order by StockID asc" #MSFT Test
$query = "Select stockID, StockTicker from [Ref].[Stock] where StockCountry = 'USA' and IsStockActive = 1 order by StockID asc"

$query | Out-File -FilePath "$RunLogOutputFile" -Append #Log the Query 

$command = $connection.CreateCommand()
$command.CommandText = $query

$result = $command.ExecuteReader()

$table = new-object "System.Data.DataTable"
$table.Load($result)


"
Table Results Workingset
------------------------
" | Out-File -FilePath "$RunLogOutputFile" -Append #Log Execution complete

$table.Rows | Out-File -FilePath "$RunLogOutputFile" -Append #Log the Query 



$table | ForEach-Object {
	$CurrentStockProcTime = Get-Date 
	$datatable.Clear()
	
	'--------------------------------------------------------------------------------------------------------' | Out-File -FilePath "$RunLogOutputFile" -Append
		
	$CurrentStockTicker = $_.StockTicker
	$CurrentStockID = $_.StockID
	
	"$CurrentStockID - $CurrentStockTicker - $CurrentStockProcTime" | Out-File -FilePath "$RunLogOutputFile" -Append
	
	
	$CurrentStockFilePath = $stockdldfilepath + $CurrentStockID + ".csv"
	
	#Old Yahoo URL       
	#$CurrentStockURL = "http://chart.finance.yahoo.com/table.csv?s=" + $CurrentStockTicker + "&a=0&b=1&c=1950&d=11&e=31&f=2025&g=d&ignore=.csv"
	#$CurrentStockURL = "https://www.google.com/finance/historical?output=csv&q=" + $CurrentStockTicker
	$CurrentStockURL = "https://query1.finance.yahoo.com/v7/finance/download/" + $CurrentStockTicker + "?period1=1597363200&period2=1597795200&interval=1d&events=history"
	"Current Stock URL - " + $CurrentStockURL | Out-File -FilePath "$RunLogOutputFile" -Append
	"Current Stock File - " + $CurrentStockFilePath | Out-File -FilePath "$RunLogOutputFile" -Append
	
	Invoke-WebRequest -Uri $CurrentStockURL -OutFile $CurrentStockFilePath -ErrorAction SilentlyContinue
	
	 	
	#Error Checking and Logging - Not the cleanest error trapping but WebRequest Errors are difficult to capture and evaluate.
	if ((Test-Path $CurrentStockFilePath) -eq 0)
	{
		$ErrorUpdateQuery = "Update Ref.Stock  Set IsStockActive = 0 where StockID = " + $CurrentStockID
		$StockLogError = "FAILED - Stock Ticker " + $CurrentStockTicker + " StockID " + $CurrentStockID + " - Stock Updated in DB as Inactive"
		
	}
	
	$command.CommandText = $ErrorUpdateQuery
	
	try {
		$command.ExecuteNonQuery() 
	}
	# Catch all other exceptions thrown by one of those commands
	catch
	{
		$StockLogError | Out-File -FilePath "$ErrorLogOutputFile" -Append
	}
	

	$StockLogError | Out-File -FilePath "$ErrorLogOutputFile" -Append
	
	#Load the Datatable
	
	$reader = New-Object System.IO.StreamReader($CurrentStockFilePath)
	if ($firstRowColumnNames -eq $true) { $null = $reader.readLine() } #Need to read out the header row in order to get to the data rows
	
	while (($line = $reader.ReadLine()) -ne $null)
	{
		$i++;
		$line = "$CurrentStockID,$Line,$CurrentStockProcTime"
		
		
		$null = $datatable.Rows.Add($line.Split($csvdelimiter)) 
		
	}
	
	
	
	$bulkcopy.WriteToServer($datatable)
	
	"$i rows have been inserted. Full process Time Elapsed $($elapsed.Elapsed.ToString())." | Out-File -FilePath "$RunLogOutputFile" -Append
	
	$datatable.Clear()
	
	}

#CleanUp
"Cleaning Up Memory Processes"| Out-File -FilePath "$RunLogOutputFile" -Append
$connection.Close()
$reader.Close();
$reader.Dispose()
$datatable.Dispose()

'--------------------------------------------------------------------------------------------------------' | Out-File -FilePath "$RunLogOutputFile" -Append

Get-Date | Out-File -FilePath "$RunLogOutputFile" -Append

"Time taken: $((Get-Date).Subtract($proc_start_time).Seconds) second(s)" | Out-File -FilePath "$RunLogOutputFile" -Append

