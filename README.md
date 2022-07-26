# go-mssql-driver-playground

```
git clone https://github.com/nini-k/go-mssql-driver-playground.git
cd go-mssql-driver-playground
go mod tidy
docker run -e "ACCEPT_EULA=1" -e "MSSQL_SA_PASSWORD=MyPass@word" -e "MSSQL_PID=Developer" -e "MSSQL_USER=SA" -p 1433:1433 -d --name=debug-mssql mcr.microsoft.com/azure-sql-edge
go test ./...
```