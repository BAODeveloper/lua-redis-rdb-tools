local StringIO = require 'StringIO'
local byte, sub, char = string.byte, string.sub, string.char
local insert, concat = table.insert, table.concat
local tostring, tonumber = tostring, tonumber
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

local VALUE_TYPE_STRING = 'string'
local VALUE_TYPE_LIST = 'list'
local VALUE_TYPE_SET = 'set'
local VALUE_TYPE_ZSET = 'zset'
local VALUE_TYPE_HASH = 'hash'

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

local function read_string(f)
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
            val = lzf_decompress(orig_val, l)
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

local function skip(f, free)
    free = tonumber(free)
    if free then
        f:read(free)
    end
end

local function read_ziplist_entry(f)
    local length, value = 0, nil
    local prev_length = read_unsigned_char(f)
    if prev_length == 254 then
        prev_length = read_unsigned_int(f)
    end
    
    local entry_header = read_unsigned_char(f)
    if bit_rshift(entry_header, 6) == 0 then
        length = bit_band(entry_header, 0x3f)
        value = f:read(length)
    elseif bit_rshift(entry_header, 6) == 1 then
        length = bit_band(entry_header, 0x3f)
        length = bit_lshift(length, 8)
        local tmp = read_unsigned_char(f)
        length = bit_bor(length, tmp)
        value = f:read(length)
    elseif bit_rshift(entry_header, 6) == 2 then
        length = read_bid_endian_unsigned_int(f)
        value = f:read(length)
    elseif bit_rshift(entry_header, 4) == 12 then
        value = read_signed_short(f)
    elseif bit_rshift(entry_header, 4) == 13 then
        value = read_signed_int(f)
    elseif bit_rshift(entry_header, 4) == 14 then
        value = read_signed_long(f)
    elseif entry_header == 240 then
        value = read_24bit_signed_number(f)
    elseif entry_header == 254 then
        value = read_signed_char(f)
    elseif entry_header >= 241 and entry_header <= 253 then
        value = entry_header - 241
    else
        -- error
    end
    return value
end

local function read_zipmap_next_length(f)
    local num = read_unsigned_char(f)
    if num < 254 then
        return num
    elseif num == 254 then
        num = read_unsigned_int(f)
        return num
    else
        return nil
    end
end

local function read_zipmap(buff)
    local num_entries = read_unsigned_char(buff)
    local info = {}
    while true do
        local next_length = read_zipmap_next_length(buff)
        if not next_length then
            break
        end

        local field = buff:read(next_length)
        next_length = read_zipmap_next_length(buff)
        if not next_length then
            break
        end

        local free = read_unsigned_char(buff)
        local value = buff:read(next_length)
        value = tonumber(value) or value

        info[tostring(field)] = value
        skip(buff, free)
    end
    return info
end

local function read_ziplist(buff)
    local zlbytes = read_unsigned_int(buff)
    local tail_offset = read_unsigned_int(buff)
    local num_entries = read_unsigned_short(buff)

    local info = {}
    for i=1,num_entries,1 do
        local val = read_ziplist_entry(buff)
        info[i] = val
    end
    local zlist_end = read_unsigned_char(buff)
    return info
end

local function read_intset(buff)
    local encoding = read_unsigned_int(buff)
    local num_entries = read_unsigned_short(buff)

    local info = {}
    for i=1,num_entries,1 do
        local entry = nil
        if encoding == 8 then
            entry = read_unsigned_long(buff)
        elseif encoding == 4 then
            entry = read_unsigned_int(buff)
        elseif encoding == 2 then
            entry = read_unsigned_short(buff)
        else
            --error
        end
        insert(info, entry)
    end
    return info
end

local function read_zset_from_ziplist(buff)
    local zlbytes = read_unsigned_int(buff)
    local tail_offset = read_unsigned_int(buff)
    local num_entries = read_unsigned_short(buff)

    num_entries = num_entries / 2

    local info = {}
    for i=1,num_entries,1 do
        local member = read_ziplist_entry(buff)
        local score = read_ziplist_entry(buff)
        info[tostring(member)] = score
    end
    local zlist_end = read_unsigned_char(buff)
    return info
end

local function read_hash_from_ziplist(buff)
    local zlbytes = read_unsigned_int(buff)
    local tail_offset = read_unsigned_int(buff)
    local num_entries = read_unsigned_short(buff)

    num_entries = num_entries / 2

    local info = {}
    for i=1,num_entries,1 do
        local field = read_ziplist_entry(buff)
        local value = read_ziplist_entry(buff)
        info[tostring(field)] = value
    end
    local zlist_end = read_unsigned_char(buff)
    return info
end

local function read_object(f, enc_type)
    local val, orig, val_type = nil, nil, nil

    if enc_type == REDIS_RDB_TYPE_STRING then
        val, orig_val = read_string(f)
        return val, orig_val, VALUE_TYPE_STRING

    elseif enc_type == REDIS_RDB_TYPE_LIST then
        local _orig = {}
        local length, orig_length = read_length(f)
        insert(_orig, orig_length)
        local info = {}
        for i=1,length,1 do
            local value, orig_value = read_string(f)
            insert(_orig, orig_value)
            insert(info, value)
        end
        return info, concat(_orig, ''), VALUE_TYPE_LIST

    elseif enc_type == REDIS_RDB_TYPE_SET then
        local _orig = {}
        local length, orig_length = read_length(f)
        insert(_orig, orig_length)
        local info = {}
        for i=1,length,1 do
            local value, orig_value = read_string(f)
            insert(_orig, orig_value)
            insert(info, value)
        end
        return info, concat(_orig, ''), VALUE_TYPE_SET

    elseif enc_type == REDIS_RDB_TYPE_ZSET then
        local _orig = {}
        local length, orig_length = read_length(f)
        insert(_orig, orig_length)
        local info = {}
        for i=1,length,1 do
            local value, orig_value = read_string(f)
            local dbl_length, orig_dbl_length = read_unsigned_char(f)
            local score = f:read(dbl_length)
            insert(_orig, orig_value)
            insert(_orig, orig_dbl_length)
            insert(_orig, score)
            info[tostring(value)] = score
        end
        return info, concat(_orig, ''), VALUE_TYPE_ZSET

    elseif enc_type == REDIS_RDB_TYPE_HASH then
        local _orig = {}
        local length, orig_length = read_length(f)
        insert(_orig, orig_length)
        local info = {}
        for i=1,length,1 do
            local field, orig_field = read_string(f)
            local value, orig_value = read_string(f)
            insert(_orig, orig_field)
            insert(_orig, orig_value)
            info[tostring(field)] = value
        end
        return info, concat(_orig, ''), VALUE_TYPE_HASH

    elseif enc_type == REDIS_RDB_TYPE_HASH_ZIPMAP then
        val, orig_val = read_string(f)
        local info = read_zipmap(StringIO:new(val))
        return info, orig_val, VALUE_TYPE_HASH

    elseif enc_type == REDIS_RDB_TYPE_LIST_ZIPLIST then
        val, orig_val = read_string(f)
        local info = read_ziplist(StringIO:new(val))
        return info, orig_val, VALUE_TYPE_LIST

    elseif enc_type == REDIS_RDB_TYPE_SET_INTSET then
        val, orig_val = read_string(f)
        local info = read_intset(StringIO:new(val))
        return info, orig_val, VALUE_TYPE_SET

    elseif enc_type == REDIS_RDB_TYPE_ZSET_ZIPLIST then
        val, orig_val = read_string(f)
        local info = read_zset_from_ziplist(StringIO:new(val))
        return info, orig_val, VALUE_TYPE_ZSET

    elseif enc_type == REDIS_RDB_TYPE_HASH_ZIPLIST then
        val, orig_val = read_string(f)
        local info = read_hash_from_ziplist(StringIO:new(val))
        return info, orig_val, info, VALUE_TYPE_HASH
    end

    return nil, nil, nil
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

    local db_num = 0

    while true do
        local data_type, orig_data_type = read_unsigned_char(f)
        local expire, ms_expire, orig_expiry = nil, nil

        if data_type == REDIS_RDB_OPCODE_EXPIRETIME_MS then
            ms_expire, orig_expiry = read_unsigned_long(f)
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

            local key, orig_key = read_string(f)
            local value, orig_value, value_type = read_object(f, data_type)

            if orig_value then
                local orig_data = {
                    orig_key = orig_key,
                    orig_expiry = orig_expiry,
                    orig_data_type = orig_data_type,
                    orig_value = orig_value,
                }
                local data = {
                    key = key,
                    expire = tonumber(expire),
                    ms_expire = tonumber(ms_expire),
                    value = value,
                }

                callback:set(key, value_type, orig_data, data)
            end
        end
    end
    f:close()
end

return l_parser

