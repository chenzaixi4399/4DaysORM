------------------------------------------------------------------------------
--                               Require                                    --
------------------------------------------------------------------------------

require('orm.class.global')
require("orm.tools.func")

local Table = require('orm.class.table')

------------------------------------------------------------------------------
--                                Constants                                 --
------------------------------------------------------------------------------
-- Global
ID = "id"
AGGREGATOR = "aggregator"
QUERY_LIST = "query_list"

-- databases types
SQLITE = "sqlite3"
ORACLE = "oracle"
MYSQL = "mysql"
POSTGRESQL = "postgresql"

------------------------------------------------------------------------------
--                              Model Settings                              --
------------------------------------------------------------------------------

if not DB then
    print("[SQL:Startup] Can't find global database settings variable 'DB'. Creating empty one.")
    DB = {}
end

DB = {
    -- ORM settings
    new = (DB.new == true),
    DEBUG = (DB.DEBUG == true),
    backtrace = (DB.backtrace == true),
    -- database settings
    type = DB.type or "sqlite3",
    -- if you use sqlite set database path value
    -- if not set a database name
    name = DB.name or "database.db",
    -- not sqlite db settings
    host = DB.host or nil,
    port = DB.port or nil,
    username = DB.username or nil,
    password = DB.password or nil
}

local sql, _connect

-- Get database by settings
if DB.type == SQLITE then
    local luasql = require("luasql.sqlite3")
    sql = luasql.sqlite3()
    _connect = sql:connect(DB.name)

elseif DB.type == MYSQL then
    -- 原 luasql 
    -- local luasql = require("luasql.mysql")
    -- sql = luasql.mysql()

    -- 改为 skynet.db.mysql
    print(DB.name, DB.username, DB.password, DB.host, DB.port)
    local mysql = require("skynet.db.mysql")
    _connect = mysql.connect(
        {
            host = DB.host,
            port = DB.port,
            database = DB.name,
            user = DB.username,
            password = DB.password,
        }
    )
    
    -- 原 luasql 
    -- _connect = sql:connect(DB.name, DB.username, DB.password, DB.host, DB.port)

elseif DB.type == POSTGRESQL then
    local luasql = require("luasql.postgres")
    sql = luasql.postgres()
    print(DB.name, DB.username, DB.password, DB.host, DB.port)
    _connect = sql:connect(DB.name, DB.username, DB.password, DB.host, DB.port)

else
    BACKTRACE(ERROR, "Database type not suported '" .. tostring(DB.type) .. "'")
end

if not _connect then
    BACKTRACE(ERROR, "Connect problem!")
end

-- if DB.new then
--     BACKTRACE(INFO, "Remove old database")

--     if DB.type == SQLITE then
--         os.remove(DB.name)
--     else
--         _connect:execute('DROP DATABASE `' .. DB.name .. '`')
--     end
-- end

------------------------------------------------------------------------------
--                               Database                                   --
------------------------------------------------------------------------------

-- Database settings
db = {
    -- Database connect instance
    connect = _connect,

    -- Execute SQL query
    execute = function (self, query)
        BACKTRACE(DEBUG, query)
        
        -- 使用 mysql.lua 中的 query 方法来执行 SQL 查询
        local result, err = self.connect:query(query)

        if result then
            return result  -- 返回查询结果
        else
            BACKTRACE(WARNING, "Wrong SQL query: " .. (err or "unknown error"))
            return nil, err  -- 返回错误信息
        end
    end,

    -- Return insert query id
    insert = function (self, query)
        local result, err = self:execute(query)  -- 使用 execute 方法执行插入
        if not result then
            return nil, err  -- 如果有错误，返回错误信息
        end
        return result.insert_id  -- 返回插入的记录 ID
    end,

    -- Get parsed data
    rows = function (self, query, own_table)
        local result, err = self:execute(query)  -- 使用 execute 方法获取结果
        local data = {}
        if not result then
            return nil, err  -- 如果执行失败，返回错误
        end

        for _, row in ipairs(result) do  -- 遍历结果集
            local current_row = {}
            for colname, value in pairs(row) do
                local current_table, column_name = string.divided_into(colname, "_")

                if current_table == own_table.__tablename__ then
                    current_row[column_name] = value  -- 填充当前行的对应列
                else
                    if not current_row[current_table] then
                        current_row[current_table] = {}
                    end

                    current_row[current_table][column_name] = value  -- 填充外部表的列
                end
            end

            table.insert(data, current_row)  -- 将当前行插入数据集
        end

        return data  -- 返回解析后的数据
    end
}

return Table
