from lib.dataflow import process


class WorkFlow:
    @staticmethod
    def process(dp: process.DataProvider, actions):
        process.DispatchCenter.dispatch(dp=dp, actions=actions)
