from lottery.bet import Bet

LENGTH_FIELD_SIZE = 4  # uint32 BE para el largo total
LEN_PREFIX_SIZE = 1    # uint8 BE para el largo de cada string


def _u32(value: int) -> bytes:
    return int(value).to_bytes(LENGTH_FIELD_SIZE, "big")


def _u32_from(data: bytes) -> int:
    return int.from_bytes(data, "big")


def encode_bet(bet: Bet) -> bytes:
    first = bet.first_name.encode()
    last = bet.last_name.encode()
    birth = bet.birthdate.encode()

    result = bytearray()
    result += _u32(bet.agency_id)
    result.append(len(first))
    result += first
    result.append(len(last))
    result += last
    result += _u32(bet.document)
    result.append(len(birth))
    result += birth
    result += _u32(bet.number)
    return bytes(result)


def decode_bet(payload: bytes) -> Bet:
    offset = 0

    agency_id = _u32_from(payload[offset:offset + LENGTH_FIELD_SIZE])
    offset += LENGTH_FIELD_SIZE

    first_len = payload[offset]
    offset += LEN_PREFIX_SIZE
    first = payload[offset:offset + first_len].decode()
    offset += first_len

    last_len = payload[offset]
    offset += LEN_PREFIX_SIZE
    last = payload[offset:offset + last_len].decode()
    offset += last_len

    document = _u32_from(payload[offset:offset + LENGTH_FIELD_SIZE])
    offset += LENGTH_FIELD_SIZE

    birth_len = payload[offset]
    offset += LEN_PREFIX_SIZE
    birth = payload[offset:offset + birth_len].decode()
    offset += birth_len

    number = _u32_from(payload[offset:offset + LENGTH_FIELD_SIZE])

    return Bet(
        agency_id=agency_id,
        first_name=first,
        last_name=last,
        document=document,
        birthdate=birth,
        number=number,
    )


def encode_wire(bet: Bet) -> bytes:
    payload = encode_bet(bet)
    return _u32(len(payload)) + payload


def decode_length(header: bytes) -> int:
    return _u32_from(header)