local sub = string.sub

local StringIO = {}

StringIO.new = function(self, str)
    local stringio = {
        str = str,
        cur = 1
    }
    setmetatable(stringio, StringIO)
    self.__index = StringIO
    return stringio
end

StringIO.read = function(self, num)
    local new_cur = self.cur + num - 1
    local result = sub(self.str, self.cur, new_cur)
    self.cur = new_cur + 1
    return result
end

return StringIO

