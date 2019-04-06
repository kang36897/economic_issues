from behave import *
from restaurant.Chain import Chain
from restaurant.Utils import compareListIgnoreOrder

@Given('we have known all possible cases related to known signals')
def step_impl(context):
    context.expected = [
            ['a'],
            ['b'],
            ['c'],
            ['a', 'b'],
            ['a', 'c'],
            ['b', 'c'],
            ['a', 'b', 'c']
        ]

@When('we are provided some signals')
def step_impl(context):
    context.signals = ['a', 'b', 'c']


@Then('chain will give us the cases correctly')
def step_impl(context):
    chain = Chain(None, 0, 100)
    result = chain.iteratePossiblePackage(input)
    expected_length = len(context.expected)
    assert expected_length == len(result)
    for i in range(expected_length):
        compareListIgnoreOrder(context.expected[i], result[i])