from __future__ import annotations

import base64

from google.protobuf import descriptor_pb2, descriptor_pool, message_factory

from parsertang.ws_native.mexc_desc import MEXC_DESCRIPTOR_BASE64


_WRAPPER_CLS = None


def get_wrapper_message_class():
    global _WRAPPER_CLS
    if _WRAPPER_CLS is not None:
        return _WRAPPER_CLS

    data = base64.b64decode(MEXC_DESCRIPTOR_BASE64)
    fds = descriptor_pb2.FileDescriptorSet.FromString(data)
    pool = descriptor_pool.DescriptorPool()
    for file_desc in fds.file:
        pool.Add(file_desc)
    desc = pool.FindMessageTypeByName("PushDataV3ApiWrapper")
    _WRAPPER_CLS = message_factory.GetMessageClass(desc)
    return _WRAPPER_CLS
