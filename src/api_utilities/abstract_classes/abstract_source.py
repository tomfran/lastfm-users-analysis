from abc import ABCMeta, abstractmethod

class AbstractSource(metaclass = ABCMeta):

	def __init__(self):
		pass
		
	@abstractmethod	
	def read(self):
		pass