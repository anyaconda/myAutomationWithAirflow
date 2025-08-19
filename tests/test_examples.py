import pytest

def test_equality():
    assert 1 == 1

def test_list():
    assert 1 in [1, 2, 3]


def tearDown():
    print('This runs after each test')

def test_example():
    print('This is a test method that will pass')


## PARAMATERIZATION IN TESTING ##

## Testing Multiple Paramaters using several tests ##
def test_eval_addition():
    assert eval("2 + 2") == 4

def test_eval_subtraction():
    assert eval("2 - 2") == 0

def test_eval_multiplication():
    assert eval("2 * 2") == 4

def test_eval_division():
    assert eval("2 / 2") == 1.0

## Testing same Multiple Paramaters using parametrize ##
@pytest.mark.parametrize("test_input, expected_output", [("2+2", 4), ("2-2", 0), ("2*2", 4), ("2/2", 1.0)])
def test_eval(test_input, expected_output):
    assert eval(test_input) == expected_output