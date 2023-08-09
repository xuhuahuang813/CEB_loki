from ortools.sat.python import cp_model
import collections


class SolutionPrinter(cp_model.CpSolverSolutionCallback):
    """Print intermediate solutions."""

    def __init__(self, variables):
        cp_model.CpSolverSolutionCallback.__init__(self)
        self.__variables = variables
        self.__num_vars = len(variables)
        self.__num_values = len(variables[0])
        self.__solution_count = 0

    def on_solution_callback(self):
        self.__solution_count += 1
        for var in range(self.__num_vars):
            for value in range(self.__num_values):
                if self.BooleanValue(self.__variables[var][value]):
                    print('var[%i]=%i' % (var, value), end=' ')
                    break
        print()

    def solution_count(self):
        return self.__solution_count
