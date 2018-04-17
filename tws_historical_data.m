%%
% TWS Historical data into PostgreSQL. 1 year at a time.
%
% Matlab 2017a w/ Database Tool Box.
%
% Copyright (c) 2018 Ken Segura.
%
% Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
% The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

%Clear !
clear

format bank;

pause('on');

%%
% Make sure you have JDBC loaded.
% javaclasspath('postgresql-42.2.2.jar'); 

%%
% Exchange Name.
% amex , nyse , nasdaq .

exchageName = ('nasdaq');

%%
% Dates. 
yearDate = ['2016'];

%%
% Mode 0 Get Ticker Data from TWS wrire it to .MAT file
% Mode 1 Create Tables for each ticker
% Mode 2 Delete Tables for each ticker
% Mode 3 Load data into tables.
% Mode 4 Print the Data. From ? DB?
scriptMode = 0;

startDateString = sprintf('01-Jan-%s',yearDate);
endDateString = sprintf('31-Dec-%s',yearDate);

disp (startDateString);
disp (endDateString);

%%
% This only matters if Mode 0 is selected. 
fileNameExchange = sprintf('%s_tickers.txt',exchageName);
fileTikerGoodName = sprintf('%s_%s_tickers_good.txt',exchageName,yearDate);

%%
% Define database name. 
dbNAME = sprintf('%s_hist_%s',exchageName,yearDate);


%%
% Connect to DB
conn = database(dbNAME,'<user>','<password>','Vendor','PostgreSQL','Server','<ip>','PortNumber',5432);

%%
%
colNames = {'DateValue';'Open';'High';'Low';'Close';'Volume';'Barcount';'Wap';'Gaps'};

%%
% Ticker Name to save.
%fileTikerName = sprintf('%s_%s_.txt',TICKER,yearDate);
%disp (fileTikerName);

%%
% Define and Clear the Good Ticker List 
%fileTikerGoodName = sprintf('%s_%s_tickers_good.txt',exchageName,yearDate);
%fileIDGTickers = fopen(fileTikerGoodName,'w');
%fclose(fileIDGTickers);

%disp (fileTikerGoodName);

    %%
    % TWS
    %
    % Numeric representation of a date
    % Open price
    % High price
    % Low price
    % Close price
    % Volume
    % Bar count
    % Weighted average price
    % Flag indicating if there are gaps in the bar

    %%
    %
    ib = ibtws('127.0.0.1',7496);
    ib.Handle;
    ibContract = ib.Handle.createContract;
    ibContract.secType = 'STK';
    ibContract.exchange = 'SMART';
    ibContract.primaryExchange = 'SMART';
    ibContract.currency = 'USD';
    
    startDateNumber = datenum(startDateString);
    endDateNumber = datenum(endDateString);
    
    %%
    % Main 
    
    %%
    % Mode 0
    if scriptMode == 0; %% Get Ticker Data from TWS wrire it to .MAT file  
    
    fileIDGTickers = fopen(fileTikerGoodName,'w'); %% Clear the good file
    fclose(fileIDGTickers);     
 
    fileIDTicker = fopen(fileNameExchange,'r');
    
    while ~feof(fileIDTicker) %While Loop
    
    InputTextTicker = textscan(fileIDTicker,'%s[\n\r]',1);
    TableTicker = InputTextTicker{1,1};
    TableTickerFileName = char(TableTicker);

    disp('TWS start. Paused 11 seconds');
    n = 11;
    pause(n);
                   
    ibContract.symbol = TableTickerFileName;
  
    exData = history(ib,ibContract,startDateNumber,endDateNumber,'TRADES','1 DAY'); 
    %exData = ('Empty');
    
    [exDataSize_m,exDataSize_n] = size(exData); 
    
    if exDataSize_m == 1
        disp(TableTicker);
        disp('No_Data');
    else
        
        disp(TableTicker);
        disp ('DATA');
        disp (exDataSize_m); 
        
        fileTikerName = sprintf('%s_%s_DATA.mat',TableTickerFileName,yearDate);
        save(fileTikerName, 'exData');

        fileTikerGoodName = sprintf('%s_%s_tickers_good.txt',exchageName,yearDate);
        fileIDGTickers = fopen(fileTikerGoodName,'a');
        fprintf(fileIDGTickers,'%s\r\n',TableTickerFileName);
        fclose(fileIDGTickers);       
                
        end; %% End for IF statement.
        
        disp('TWS end.');
         
    end; %% End While Loop
    fclose(fileIDTicker);     
    end; %% End Mode 0 IF statement.        

    
    %%
    % Mode 1
    if scriptMode == 1; %% Create Table for Each Ticker
    fileIDGTickers = fopen(fileTikerGoodName,'r');

    while ~feof(fileIDGTickers) %While Loop.
    
    InputTextTicker = textscan(fileIDGTickers,'%s[\n\r]',1);
    TableTicker = InputTextTicker{1,1};
    TableTickerFileName = char(TableTicker);    
    
    % Load .mat file
    fileNameB = sprintf('%s_%s_DATA.mat',TableTickerFileName,yearDate);
    load(fileNameB, 'exData');

    disp (fileNameB);
    
    [exDataSize_m,exDataSize_n] = size(exData);
    disp (exDataSize_m);
    
    if exDataSize_m == 1
        disp('No_Data');
    else
        disp('Create Table');        
        
            sqlqueryA = 'CREATE TABLE';
            sqlqueryB = '(RecordNumber SERIAL,DateValue DATE, Open DECIMAL, High DECIMAL,  Low DECIMAL, Close DECIMAL, Volume INT, Barcount INT, Wap DECIMAL, Gaps INT)';
            TableTickerMod = sprintf(' %s_T',TableTickerFileName);
            sqlqueryC = [sqlqueryA  TableTickerMod sqlqueryB];
 
            disp (sqlqueryC);
            
            curs = exec(conn,sqlqueryC);
            disp (curs);
            
            MessageCurs = char(curs.Message);
            if MessageCurs[1,1] == ('E') 
                disp('---------------Error--------------');
                disp(MessageCurs); 
                disp('---------------Error--------------');

                fileIDError = fopen('Error_Log_CREATE_TABLE.txt','a');
                fprintf(fileIDError,'%s\r\n',MessageCurs);
                fclose(fileIDError);
                             
            end; %End Message Error
    end; % If DataSize
        
    end; %% End While loop.
    fclose(fileIDGTickers);
    end; %% End Mode 1 IF statement.
    
    %%
    % Mode 2
    if scriptMode == 2; %% Delete table for each ticker

    fileIDGTickers = fopen(fileTikerGoodName,'r');

    while ~feof(fileIDGTickers) %While Loop.
    
    InputTextTicker = textscan(fileIDGTickers,'%s[\n\r]',1);
    TableTicker = InputTextTicker{1,1};
    TableTickerFileName = char(TableTicker);    

        disp('Drop Table');        
        
            sqlqueryA = 'DROP TABLE';
            sqlqueryB  = '';
            TableTickerMod = sprintf(' %s_T',TableTickerFileName);
            sqlqueryC = [sqlqueryA  TableTickerMod];

            disp (sqlqueryC);
            
            curs = exec(conn,sqlqueryC);
            disp (curs);
            
            MessageCurs = char(curs.Message);
            if MessageCurs[1,1] == ('E') 
                disp('---------------Error--------------');
                disp(MessageCurs); 
                disp('---------------Error--------------');

                fileIDError = fopen('Error_Log_CREATE_TABLE.txt','a');
                fprintf(fileIDError,'%s\r\n',MessageCurs);
                fclose(fileIDError);
                             

            end; %End Message Error
        
    end; %% End While loop.
    fclose(fileIDGTickers);        
    end; %% End Mode 2 IF statement.    
    
    %%
    % Mode 3
    if scriptMode == 3; %% Insert data from .MAT file
    fileIDGTickers = fopen(fileTikerGoodName,'r');

    while ~feof(fileIDGTickers) %While Loop.
    
    InputTextTicker = textscan(fileIDGTickers,'%s[\n\r]',1);
    TableTicker = InputTextTicker{1,1};
    TableTickerFileName = char(TableTicker);    
    
    % Load .mat file
    fileNameB = sprintf('%s_%s_DATA.mat',TableTickerFileName,yearDate);
    load(fileNameB, 'exData');
    
    [exDataSize_m,exDataSize_n] = size(exData);
    disp (exDataSize_m);
    
    if exDataSize_m == 1
        disp('No_Data');
    else
        disp('Insert Table');
            
            TableTickerMod = sprintf(' %s_T',TableTickerFileName);
            insert(conn, TableTickerMod, colNames, exData);           
            
    end; % If DataSize
        
    end; %% End While loop.
    fclose(fileIDGTickers);     
    
    end; %% End Mode 3 IF statement.
    
    
    %%
    % Mode 4
    if scriptMode == 4; %% Get Ticker Data from TWS wrire it to .MAT file

        fileIDGTickers = fopen(fileTikerGoodName,'r');

    while ~feof(fileIDGTickers) %While Loop.
    
    InputTextTicker = textscan(fileIDGTickers,'%s[\n\r]',1);
    TableTicker = InputTextTicker{1,1};
    TableTickerFileName = char(TableTicker);    
    
    % Load .mat file
    fileNameB = sprintf('%s_%s_DATA.mat',TableTickerFileName,yearDate);
    load(fileNameB, 'exData');

    disp (fileNameB);
    
    [exDataSize_m,exDataSize_n] = size(exData);
    disp (exDataSize_m);
    
    if exDataSize_m == 1
        disp('No_Data');
    else
        disp('Create Table');        
        
            sqlqueryA = 'CREATE TABLE';
            sqlqueryB = '(RecordNumber SERIAL,DateValue DATE, Open DECIMAL, High DECIMAL,  Low DECIMAL, Close DECIMAL, Volume INT, Barcount INT, Wap DECIMAL, Gaps INT)';
            TableTickerMod = sprintf(' %s_T',TableTickerFileName);
            sqlqueryC = [sqlqueryA  TableTickerMod sqlqueryB];
 
            disp (sqlqueryC);
            
            curs = exec(conn,sqlqueryC);
            disp (curs);
            
            MessageCurs = char(curs.Message);
            if MessageCurs[1,1] == ('E') 
                disp('---------------Error--------------');
                disp(MessageCurs); 
                disp('---------------Error--------------');

                fileIDError = fopen('Error_Log_CREATE_TABLE.txt','a');
                fprintf(fileIDError,'%s\r\n',MessageCurs);
                fclose(fileIDError);
                             
            end; %End Message Error
    end; % If DataSize
        
    end; %% End While loop.
    fclose(fileIDGTickers);
    
    end; %% End Mode 4 IF statement.
    
    
    %%
    %

    
 close(conn);
