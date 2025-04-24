#Dictionary mapping rule types to the list of prioritized column names.
#used to determine which columns shoukd be checked for presence and in what order.
priority_rule_dict = {
    4: '<primary_pan_acct_nbr15>',
    3: '<primary_pan_acct_nbr13>',
    2: '<primary_pan_acct_nbr11>',
    1: '<cust_id>'
}
#Dictionary assigning priority scores to each column
#Higher value means higher priority when checking column presence.
rule_priority_dict = {
    '<primary_pan_acct_nbr15>': 4,
    '<primary_pan_acct_nbr13>': 3,
    '<primary_pan_acct_nbr11>': 2,
    '<cust_id>':1
}
