# ggsql-oracle-adb

This repo explores how to use ggsql with Oracle Database. In this project we use the oracledb Python library to connect to an Oracle Autonomous Database.

The example class, ggsqlOracle, accepts an oracledb connection object - so this code should be re-usable for connections to different versions and installations of Oracle Database. 

There is also an example Cline Skill included to help automate creation of plots using Natural Language. This assumes you are using the SQLcl MCP tool to discover the correct SQL statement for your Oracle data.