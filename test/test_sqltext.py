import pytest

from sqlblock.sqltext import SQL


def test_sqltext():
    name, age = "abc", 28

    s = SQL("[{name},{age},{age + 10}];")
    s += SQL(f"<{{name}},{age}>")
    assert len(s._segments) == 10

    stmt, vals = s.get_statment()
    print(stmt, vals)
    assert stmt == '[$1,$2,$3];<$4,28>'
    assert vals == ['abc', 28, 38, 'abc']

    s0 = SQL('[')
    s1 = s0 + '{age}' + ']'
    stmt, vals = s1.get_statment()
    assert stmt == '[$1]' and vals == [28]
    stmt, vals = s0.get_statment()
    assert stmt == '[' and vals == []

    s0 = SQL('[')
    s0 += '{age}' + ']'
    stmt, vals = s0.get_statment()
    assert stmt == '[$1]' and vals == [28]

"""
TODO: 安全、简单明了起见，不在使用后期参数替代，完全使用上下文中的变量
"""
# def test_sqltext2():
#     a, b = 1, 2
#     t = SQL("{g}{g+1}")
#     s = SQL("{a}{b}{c}[{t}]")
#     with pytest.raises(NameError):
#         stmt, vals = s.get_statment()
#     stmt, vals = s.get_statment(params=dict(b=10, c=20, g=30))
#     assert stmt == '$1$2$3[$4$5]'
#     assert vals[4] == 31
#     print(stmt, vals)


# def test_sqltext3():
#     s0 = SQL('[{age}]', vars=dict(age=18))
#     s0 += SQL('[{b}]', vars=dict(b=100))
#     stmt, vals = s0.get_statment()
#     assert stmt == '[$1][$2]' and vals == [18, 100]


def test_sqltext4():
    s = SQL()
    for i in range(1, 5):
        s += SQL("[{i+1}]")
    _, vals = s.get_statment()
    assert vals == [2, 3, 4, 5]


def test_fragments():

    offset = 0
    limit = 1
    
    sqlf_offset = SQL("OFFSET {offset}" ) if offset is not None else SQL()
    sqlf_limit  = SQL("LIMIT {limit}" ) if limit is not None else SQL()

    nums = [11, 22]

    sql = SQL("""
    SELECT sn FROM (VALUES ({nums[0]}), ({nums[1]})) AS t(sn) 
    {sqlf_offset} {sqlf_limit}
    """)

    stmt, vals = sql.get_statment()

    print(stmt, vals)
