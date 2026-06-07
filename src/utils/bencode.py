def decode(data: bytes):
    def decode_item(data, index):
        if data[index:index+1] == b'i':
            end = data.index(b'e', index + 1)
            return int(data[index+1:end]), end + 1
        elif data[index:index+1] == b'l':
            res = []
            index += 1
            while data[index:index+1] != b'e':
                item, index = decode_item(data, index)
                res.append(item)
            return res, index + 1
        elif data[index:index+1] == b'd':
            res = {}
            index += 1
            while data[index:index+1] != b'e':
                key, index = decode_item(data, index)
                val, index = decode_item(data, index)
                res[key] = val
            return res, index + 1
        else:
            colon = data.index(b':', index)
            length = int(data[index:colon])
            start = colon + 1
            return data[start:start+length], start + length

    return decode_item(data, 0)[0]

def encode(item) -> bytes:
    if isinstance(item, int):
        return f"i{item}e".encode()
    elif isinstance(item, bytes):
        return f"{len(item)}:".encode() + item
    elif isinstance(item, str):
        encoded = item.encode('utf-8')
        return f"{len(encoded)}:".encode() + encoded
    elif isinstance(item, list):
        return b"l" + b"".join(encode(i) for i in item) + b"e"
    elif isinstance(item, dict):
        # Keys must be sorted as byte strings in bencode
        sorted_items = sorted(item.items(), key=lambda x: x[0] if isinstance(x[0], bytes) else x[0].encode('utf-8'))
        return b"d" + b"".join(encode(k) + encode(v) for k, v in sorted_items) + b"e"
    else:
        raise ValueError(f"Unsupported type {type(item)}")
