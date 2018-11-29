#!/usr/bin/env python
# -*- coding: utf-8 -*-
class RunConfig(object):
    def __init__(self):
        pass
    
    def parse(self,filename):
        pass
    
    @classmethod
    def instance(cls):
        if not hasattr(cls,"_instance"):
            cls._instance = cls()
        return cls._instance

    @classmethod
    def initialized(cls):
        return hasattr(cls,"_instance")
    
