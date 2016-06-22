from sqlalchemy_teradata.compiler import TeradataTypeCompiler as tdtc
from sqlalchemy_teradata.dialect import TeradataDialect as tdd
from sqlalchemy_teradata.types import ( IntervalYear, IntervalYearToMonth, IntervalMonth,
                                  IntervalDay, IntervalDayToHour, IntervalDayToMinute,
                                  IntervalDayToSecond, IntervalHour, IntervalHourToMinute,
                                  IntervalHourToSecond, IntervalMinute, IntervalMinuteToSecond,
                                  IntervalSecond)

from sqlalchemy.testing import fixtures

class TestCompileTDInterval(fixtures.TestBase):
   """
    Test the compilation of the  Teradata Interval/ Interval to types
   """

   def setup(self):

     # Teradata Type Compiler using Teradata Dialect to compile types
     self.comp = tdtc(tdd)

   def test_defaults(self):

    assert self.comp.process(IntervalYear()) == 'INTERVAL YEAR'
    assert self.comp.process(IntervalYearToMonth()) == 'INTERVAL YEAR TO MONTH'
    assert self.comp.process(IntervalMonth()) == 'INTERVAL MONTH'
    assert self.comp.process(IntervalDay()) == 'INTERVAL DAY'
    assert self.comp.process(IntervalDayToHour()) == 'INTERVAL DAY TO HOUR'
    assert self.comp.process(IntervalDayToMinute()) == 'INTERVAL DAY TO MINUTE'
    assert self.comp.process(IntervalDayToSecond()) == 'INTERVAL DAY TO SECOND'
    assert self.comp.process(IntervalHour()) == 'INTERVAL HOUR'
    assert self.comp.process(IntervalHourToMinute()) == 'INTERVAL HOUR TO MINUTE'
    assert self.comp.process(IntervalHourToSecond()) == 'INTERVAL HOUR TO SECOND'
    assert self.comp.process(IntervalMinute()) == 'INTERVAL MINUTE'
    assert self.comp.process(IntervalMinuteToSecond()) == 'INTERVAL MINUTE TO SECOND'
    assert self.comp.process(IntervalSecond()) == 'INTERVAL SECOND'

   def test_interval(self):

     for prec in range(1,5):
       assert self.comp.process(IntervalYear(prec)) ==  'INTERVAL YEAR({})'.format(prec)
       assert self.comp.process(IntervalYearToMonth(prec)) == \
                                                        'INTERVAL YEAR({}) TO MONTH'.format(prec)
       assert self.comp.process(IntervalMonth(prec)) == 'INTERVAL MONTH({})'.format(prec)
       assert self.comp.process(IntervalDay(prec))   == 'INTERVAL DAY({})'.format(prec)
       assert self.comp.process(IntervalDayToHour(prec)) == \
                                                        'INTERVAL DAY({}) TO HOUR'.format(prec)
       assert self.comp.process(IntervalDayToMinute(prec)) == \
                                                      'INTERVAL DAY({}) TO MINUTE'.format(prec)
       assert self.comp.process(IntervalHour(prec)) == 'INTERVAL HOUR({})'.format(prec)
       assert self.comp.process(IntervalHourToMinute(prec)) == \
                                                     'INTERVAL HOUR({}) TO MINUTE'.format(prec)
       assert self.comp.process(IntervalMinute(prec)) == 'INTERVAL MINUTE({})'.format(prec)
       assert self.comp.process(IntervalSecond(prec)) == 'INTERVAL SECOND({})'.format(prec)

   def test_interval_frac(self):
     """
      Test valid ranges of precision (prec) and fractional second precision (fsec)
     """
     for prec in range(1,5):
       for fsec in range(0,7):
         assert self.comp.process(IntervalDayToSecond(prec, fsec)) == \
             'INTERVAL DAY({}) TO SECOND({})'.format(prec, fsec)

         assert self.comp.process(IntervalHourToSecond(prec, fsec)) == \
             'INTERVAL HOUR({}) TO SECOND({})'.format(prec, fsec)

         assert self.comp.process(IntervalMinuteToSecond(prec, fsec)) == \
             'INTERVAL MINUTE({}) TO SECOND({})'.format(prec, fsec)

         assert self.comp.process(IntervalSecond(prec, fsec)) == \
             'INTERVAL SECOND({}, {})'.format(prec, fsec)

