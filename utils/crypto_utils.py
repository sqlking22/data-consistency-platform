#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Time    : 2026/1/9 16:15
# @Author  : hejun

import base64
import os
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad,pad
from config.settings import ENCRYPT_KEY  # 需要在settings.py中配置ENCRYPT_KEY

# def decrypt(encrypted_text: str) -> str:
#     """AES解密数据库密码"""
#     try:
#         key = ENCRYPT_KEY.encode('utf-8')
#         cipher = AES.new(key, AES.MODE_ECB)
#         decrypted = unpad(cipher.decrypt(base64.b64decode(encrypted_text)), AES.block_size)
#         return decrypted.decode('utf-8')
#     except Exception as e:
#         raise ValueError(f"密码解密失败：{str(e)}")

def encrypt(plain_text: str) -> str:
    key = ENCRYPT_KEY.encode('utf-8')
    iv = os.urandom(AES.block_size)  # 随机IV
    cipher = AES.new(key, AES.MODE_CBC, iv)
    encrypted = cipher.encrypt(pad(plain_text.encode('utf-8'), AES.block_size))
    return base64.b64encode(iv + encrypted).decode('utf-8')

def decrypt(encrypted_text: str) -> str:
    key = ENCRYPT_KEY.encode('utf-8')
    encrypted_data = base64.b64decode(encrypted_text)
    iv = encrypted_data[:AES.block_size]
    ciphertext = encrypted_data[AES.block_size:]
    cipher = AES.new(key, AES.MODE_CBC, iv)
    return unpad(cipher.decrypt(ciphertext), AES.block_size).decode('utf-8')