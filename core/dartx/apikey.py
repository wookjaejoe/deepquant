import random

import requests


class OpenDartApiKey:
    _api_keys = [
        "3835de6f6564a072832cc4ed390fbcdf6a490152",
        "cc6919fa416d9841ddf1adf125f9f0206c1ca1fe",
        "45e34a1018d3716298c7927aa9da12743201019b",
        "21f00a16642b031061f2bf4f2433bad3ad7f7a6b",
        "da5d1d555f4f018c29836b211f3280e72d220755",
        "3cdc256faa5e5c0cbce7723aaf4d167b8ef420d9",
        "59a47be6f56893c06dbea03b168560a835dbb222",
        "25f0bcd758b2fdd78e6c0c03592376c849080334",
        "5da06f9b1be75385e04b7699ec6b5236b7c3355c",
        "22ea11027b0d0c6e8d978b58d2a0cd9e3be45ae2",
        "ce0d3e890ec89803361898e69209afdea7a49958",
        "33fea3a759dfccaf4f89d3d9ce77b33e934f4802",
        "82ce253f14dc8fc47e89007aed868d6d1b919b4a",
        "efada4c215f05441941633ae0ea871c2f5ae0f23"
    ]
    _removed = []
    _i = 0

    random.shuffle(_api_keys)

    @classmethod
    def next(cls):
        valid_keys = [k for k in cls._api_keys if k not in cls._removed]
        cls._i += 1
        i = cls._i % len(valid_keys)
        return valid_keys[i]

    @classmethod
    def remove(cls, api_key):
        cls._removed.append(api_key)

    @classmethod
    def remove_invalid_keys(cls):
        for key in cls._api_keys:
            if not cls.validate(key):
                cls.remove(key)

    @classmethod
    def validate(cls, api_key):
        res = requests.get(
            "https://opendart.fss.or.kr/api/company.json",
            params={"crtfc_key": api_key, "corp_code": "00126380"}
        )

        return not res.json()["status"] in ["011", "020"]


OpenDartApiKey.remove_invalid_keys()
