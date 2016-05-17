local byte, sub, char = string.byte, string.sub, string.char
local insert, concat = table.insert, table.concat
local open = io.open
local bit = require 'bit'
local bit_band, bit_bor, bit_rshift, bit_lshift = bit.band, bit.bor, bit.rshift, bit.lshift

local REDIS_RDB_6BITLEN = 0
local REDIS_RDB_14BITLEN = 1
local REDIS_RDB_32BITLEN = 2
local REDIS_RDB_ENCVAL = 3

local REDIS_RDB_OPCODE_EXPIRETIME_MS = 252
local REDIS_RDB_OPCODE_EXPIRETIME = 253
local REDIS_RDB_OPCODE_SELECTDB = 254
local REDIS_RDB_OPCODE_EOF = 255

local REDIS_RDB_TYPE_STRING = 0
local REDIS_RDB_TYPE_LIST = 1
local REDIS_RDB_TYPE_SET = 2
local REDIS_RDB_TYPE_ZSET = 3
local REDIS_RDB_TYPE_HASH = 4
local REDIS_RDB_TYPE_HASH_ZIPMAP = 9
local REDIS_RDB_TYPE_LIST_ZIPLIST = 10
local REDIS_RDB_TYPE_SET_INTSET = 11
local REDIS_RDB_TYPE_ZSET_ZIPLIST = 12
local REDIS_RDB_TYPE_HASH_ZIPLIST = 13

local REDIS_RDB_ENC_INT8 = 0
local REDIS_RDB_ENC_INT16 = 1
local REDIS_RDB_ENC_INT32 = 2
local REDIS_RDB_ENC_LZF = 3

local function get_number(data)
    local v = 0
    for i=#data,1,-1 do
        local x = byte(sub(data, i, i))
        v = v * 256 + x
    end
    return v
end

local function read_signed_char(f)
    local data = f:read(1) 
    return byte(data), data
end

local function read_unsigned_char(f)            
    local data = f:read(1) 
    return byte(data), data
end

local function read_signed_short(f)
    local data = f:read(2)
    return get_number(data), data
end

local function read_unsigned_short(f)
    local data = f:read(2)
    return get_number(data), data
end

local function read_signed_int(f)
    local data = f:read(4)
    return get_number(data), data
end

local function read_unsigned_int(f)             
    local data = f:read(4)
    return get_number(data), data
end

local function read_bid_endian_unsigned_int(f)  
    local data = f:read(4)
    return get_number(data), data
end

local function read_24bit_signed_number(f)      
    local data = f:read(3)
    local sft_data = get_number('0' .. data)
    return bit_rshift(sft_data, 8), data
end

local function read_signed_long(f)              
    local data = f:read(8)
    return get_number(data), data
end

local function read_unsigned_long(f)            
    local data = f:read(8)
    return get_number(data), data
end
--------------------------------------------
local function str2array(str)
    local array = {}
    for i=1,#str,1 do
        local x = sub(str, i, i)
        insert(array, byte(x))
    end
    return array
end

local function array2str(array)
    local str = {}
    for i=1,#array,1 do
        insert(str, char(array[i]))
    end
    return concat(str, '')
end

local function lzf_decompress(compressed, expected_length)
    local in_stream = str2array(compressed)
    local in_len = #in_stream
    local in_index = 1
    local out_stream = {}
    local out_index = 1

    while in_index < in_len do
        local ctrl = in_stream[in_index]
        in_index = in_index + 1
        if ctrl < 32 then
            for x=1,ctrl+1,1 do
                insert(out_stream, in_stream[in_index])
                in_index = in_index + 1
                out_index = out_index + 1
            end
        else
            local length = bit_rshift(ctrl, 5)
            if length == 7 then
                length = length + in_stream[in_index]
                in_index = in_index + 1
            end
            local _tmp = bit_lshift(bit_band(ctrl, 0x1f), 8)
            local ref = out_index - _tmp - in_stream[in_index] - 1
            in_index = in_index + 1
            for i=1,length+2,1 do
                insert(out_stream, out_stream[ref])
                ref = ref + 1
                out_index = out_index + 1
            end
        end
    end

    if #out_stream ~= expected_length then
        return nil, 'err'
    end
    return array2str(out_stream)
end

local function ntohl(f)
    local val, orig_val = read_unsigned_int(f)
    local new_val = 0
    -- new_val = new_val | ((val & 0x000000ff) << 24)
    new_val = bit_bor(new_val, bit_lshift(bit_band(val, 0x000000ff), 24))
    -- new_val = new_val | ((val & 0xff000000) >> 24)
    new_val = bit_bor(new_val, bit_rshift(bit_band(val, 0xff000000), 24))
    -- new_val = new_val | ((val & 0x0000ff00) << 8)
    new_val = bit_bor(new_val, bit_lshift(bit_band(val, 0x0000ff00), 8))
    -- new_val = new_val | ((val & 0x00ff0000) >> 8)
    new_val = bit_bor(new_val, bit_rshift(bit_band(val, 0x00ff0000), 8))
    return new_val, orig_val
end

local function read_length_with_encoding(f)
    local length = 0
    local is_encoded = false
    local bts = {}
    local data, orig_data = read_unsigned_char(f)
    insert(bts, orig_data)
    local enc_type = bit_rshift(bit_band(data, 0xC0), 6)
    if enc_type == REDIS_RDB_ENCVAL then
        is_encoded = true
        length = bit_band(data, 0x3F)
    elseif enc_type == REDIS_RDB_6BITLEN then
        length = bit_band(data, 0x3F)
    elseif enc_type == REDIS_RDB_14BITLEN then
        local data1, orig_data1 = read_unsigned_char(f)
        insert(bts, orig_data1)
        local _tmp = bit_lshift(bit_band(data, 0x3F), 8)
        length = bit_bor(_tmp, data1)
    else
        length,  orig_length = ntohl(f)
        insert(bts, orig_length)
    end
    return length, is_encoded, bts
end

local function read_length(f)
    local length, _, bts = read_length_with_encoding(f)
    return length, concat(bts, '')
end

local function read_string(f, is_key)
    local length, is_encoded, bts = read_length_with_encoding(f)
    local val, orig_val = nil, nil
    if is_encoded then
        if length == REDIS_RDB_ENC_INT8 then
            val, orig_val = read_signed_char(f)
            insert(bts, orig_val)
        elseif length == REDIS_RDB_ENC_INT16 then
            val, orig_val = read_signed_short(f)
            insert(bts, orig_val)
        elseif length == REDIS_RDB_ENC_INT32 then
            val, orig_val = read_signed_int(f)
            insert(bts, orig_val)
        elseif length == REDIS_RDB_ENC_LZF then
            local clen, orig_clen = read_length(f)
            local l, orig_l = read_length(f)
            orig_val = f:read(clen)
            if is_key then
                val = lzf_decompress(orig_val, l)
            end
            insert(bts, orig_clen)
            insert(bts, orig_l)
            insert(bts, orig_val)
        end
    else
        val = f:read(length)
        insert(bts, val)
    end
    return val, concat(bts, '')
end

-------------------------------------------

local _callback = {
    __index = function()
        return function() return nil end
    end
}

-- TODO
-- add callback
local function read_object(f, enc_type)
    local val, orig = nil, nil

    if enc_type == REDIS_RDB_TYPE_STRING then
        val, orig_val = read_string(f)
        return val, orig_val

    elseif enc_type == REDIS_RDB_TYPE_LIST then
        local _orig = {}
        local length, orig_length = read_length(f)
        insert(_orig, orig_length)
        for i=1,length,1 do
            local value, orig_value = read_string(f)
            insert(_orig, orig_value)
        end
        return nil, concat(_orig, '')

    elseif enc_type == REDIS_RDB_TYPE_SET then
        local _orig = {}
        local length, orig_length = read_length(f)
        insert(_orig, orig_length)
        for i=1,length,1 do
            local value, orig_value = read_string(f)
            insert(_orig, orig_value)
        end
        return nil, concat(_orig, '')

    elseif enc_type == REDIS_RDB_TYPE_ZSET then
        local _orig = {}
        local length, orig_length = read_length(f)
        insert(_orig, orig_length)
        for i=1,length,1 do
            local value, orig_value = read_string(f)
            local dbl_length, orig_dbl_length = read_unsigned_char(f)
            local score = f:read(dbl_length)
            insert(_orig, orig_value)
            insert(_orig, orig_dbl_length)
            insert(_orig, score)
        end
        return nil, concat(_orig, '')

    elseif enc_type == REDIS_RDB_TYPE_HASH then
        local _orig = {}
        local length, orig_length = read_length(f)
        insert(_orig, orig_length)
        for i=1,length,1 do
            local field, orig_field = read_string(f)
            local value, orig_value = read_string(f)
            insert(_orig, orig_field)
            insert(_orig, orig_value)
        end
        return nil, concat(_orig, '')

    elseif enc_type == REDIS_RDB_TYPE_HASH_ZIPMAP then
        val, orig_val = read_string(f)
        return val, orig_val

    elseif enc_type == REDIS_RDB_TYPE_LIST_ZIPLIST then
        val, orig_val = read_string(f)
        return val, orig_val

    elseif enc_type == REDIS_RDB_TYPE_SET_INTSET then
        val, orig_val = read_string(f)
        return val, orig_val

    elseif enc_type == REDIS_RDB_TYPE_ZSET_ZIPLIST then
        val, orig_val = read_string(f)
        return val, orig_val

    elseif enc_type == REDIS_RDB_TYPE_HASH_ZIPLIST then
        val, orig_val = read_string(f)
        return val, orig_val
    end

    return nil, nil
end
--------------------------------------------

local l_parser = {}

-- parser
function l_parser:parse(filename, callback)
    callback = callback or {}
    setmetatable(callback, _callback)
    local f = open(filename, 'rb')

    local magic_string = f:read(5)
    local rdb_version = f:read(4)

    local db_number = 0

    while true do
        local data_type, orig_data_type = read_unsigned_char(f)
        local expire, orig_expiry = nil, nil

        if data_type == REDIS_RDB_OPCODE_EXPIRETIME_MS then
            expire, orig_expiry = read_unsigned_long(f)
            orig_expiry = orig_data_type .. orig_expiry
            data_type, orig_data_type = read_unsigned_char(f)
        elseif data_type == REDIS_RDB_OPCODE_EXPIRETIME then
            expire, orig_expiry = read_unsigned_int(f)
            orig_expiry = orig_data_type .. orig_expiry
            data_type, orig_data_type = read_unsigned_char(f)
        end

        local db_num, orig_db_num = nil, nil
        if data_type == REDIS_RDB_OPCODE_SELECTDB then
            db_num, orig_db_num = read_length(f)
        else
            if data_type == REDIS_RDB_OPCODE_EOF then
                break
            end

            local key, orig_key = read_string(f, true)
            local value, orig_value = read_object(f, data_type)

            -- TODO
            if orig_value then
                callback:set(key, orig_key, orig_expiry, orig_data_type, orig_value)
            end
        end
    end
    f:close()
end

return l_parser

