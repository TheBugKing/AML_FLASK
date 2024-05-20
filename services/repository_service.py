from utils.dbo import Dbo


class LLMRepositoryService(Dbo):
    def __init__(self):
        # initialize super constructor of Dbo class
        # use cursor object inherited from dbo to run queries
        super().__init__()
