def has_lucky_number(nums):
    return any([num % 7 == 0 for num in nums])
