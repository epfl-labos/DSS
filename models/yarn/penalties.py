#!/usr/bin/env python

from math import sqrt
from abc import ABCMeta, abstractmethod, abstractproperty
from utils import PEnum

YarnPenalty = PEnum("YarnPenalty", "NULL POWER1 POWER2 SQUAREROOT STEP SAWTOOTH SPARKWC")


class Penalty(object):
    __metaclass__ = ABCMeta

    def __str__(self):
        return self.__class__.__name__

    @abstractmethod
    def get_penalized_runtime(self, ideal, available, running_time):
        raise NotImplementedError()


def get_penalty(penalty, initial_bump=0):
    if penalty is YarnPenalty.NULL:
        return NullPenalty()
    elif penalty is YarnPenalty.POWER1:
        return Power1Penalty()
    elif penalty is YarnPenalty.POWER2:
        return Power2Penalty()
    elif penalty is YarnPenalty.SQUAREROOT:
        return SquarerootPenalty()
    elif penalty is YarnPenalty.STEP:
        return StepPenalty(initial_bump)
    elif penalty is YarnPenalty.SAWTOOTH:
        return SawtoothPenalty(initial_bump)
    elif penalty is YarnPenalty.SPARKWC:
        return SparkWCPenalty(initial_bump)
    else:
        raise Exception("Undefined penalty type: " + penalty)


class BumpPenalty(Penalty):
    __metaclass__ = ABCMeta

    def __init__(self, initial_bump):
        self.initial_bump = initial_bump

    def __str__(self):
        return "".join([super(BumpPenalty, self).__str__(), " ", "IB", str(self.initial_bump)])

    @abstractmethod
    def get_penalized_runtime(self, ideal, available, running_time):
        raise NotImplementedError()


class NullPenalty(Penalty):
    def get_penalized_runtime(self, ideal, available, running_time):
        return running_time


class Power1Penalty(Penalty):
    def get_penalized_runtime(self, ideal, available, running_time):
        ratio = float(ideal.memory_mb) / available.memory_mb
        return int(ratio * running_time)


class Power2Penalty(Penalty):
    def get_penalized_runtime(self, ideal, available, running_time):
        ratio = float(ideal.memory_mb) / available.memory_mb
        return int(ratio * ratio * running_time)


class SquarerootPenalty(Penalty):
    def get_penalized_runtime(self, ideal, available, running_time):
        ratio = float(ideal.memory_mb) / available.memory_mb
        return int(sqrt(ratio) * running_time)


class StepPenalty(BumpPenalty):
    def get_penalized_runtime(self, ideal, available, running_time):
        if available.memory_mb == ideal.memory_mb:
            return running_time

        return int(self.initial_bump * running_time)


class PercentagePenalty(BumpPenalty):
    __metaclass__ = ABCMeta

    def __init__(self, initial_bump):
        BumpPenalty.__init__(self, initial_bump)

    @abstractproperty
    def values(self):
        raise NotImplementedError()

    def get_penalized_runtime(self, ideal, available, running_time):
        if available.memory_mb == ideal.memory_mb:
            return running_time

        ratio = available.memory_mb * 100 / ideal.memory_mb
        additional_penalty_before_ideal = running_time * (self.initial_bump - 1)
        result = self.values[ratio] * additional_penalty_before_ideal + running_time

        return int(result)


class SawtoothPenalty(PercentagePenalty):
    def __init__(self, initial_bump):
        PercentagePenalty.__init__(self, initial_bump)

        sawtooth_values = [0] * 101  # 100 percentages
        sawtooth_values[1] = 1.90996
        sawtooth_values[2] = 1.89957
        sawtooth_values[3] = 1.76883
        sawtooth_values[4] = 1.67965
        sawtooth_values[5] = 1.6
        sawtooth_values[6] = 1.37922
        sawtooth_values[7] = 1.329
        sawtooth_values[8] = 1.19913
        sawtooth_values[9] = 1.16883
        sawtooth_values[10] = 0.899567
        sawtooth_values[11] = 0.98961
        sawtooth_values[12] = 0.959307
        sawtooth_values[13] = 0.909957
        sawtooth_values[14] = 0.979221
        sawtooth_values[15] = 0.899567
        sawtooth_values[16] = 0.959307
        sawtooth_values[17] = 0.849351
        sawtooth_values[18] = 0.899567
        sawtooth_values[19] = 0.949784
        sawtooth_values[20] = 0.8
        sawtooth_values[21] = 0.839827
        sawtooth_values[22] = 0.879654
        sawtooth_values[23] = 0.919481
        sawtooth_values[24] = 0.959307
        sawtooth_values[25] = 1
        sawtooth_values[26] = 0.779221
        sawtooth_values[27] = 0.809524
        sawtooth_values[28] = 0.839827
        sawtooth_values[29] = 0.869264
        sawtooth_values[30] = 0.899567
        sawtooth_values[31] = 0.92987
        sawtooth_values[32] = 0.959307
        sawtooth_values[33] = 0.98961
        sawtooth_values[34] = 0.679654
        sawtooth_values[35] = 0.699567
        sawtooth_values[36] = 0.719481
        sawtooth_values[37] = 0.739394
        sawtooth_values[38] = 0.759307
        sawtooth_values[39] = 0.779221
        sawtooth_values[40] = 0.8
        sawtooth_values[41] = 0.819913
        sawtooth_values[42] = 0.839827
        sawtooth_values[43] = 0.85974
        sawtooth_values[44] = 0.879654
        sawtooth_values[45] = 0.899567
        sawtooth_values[46] = 0.919481
        sawtooth_values[47] = 0.939394
        sawtooth_values[48] = 0.959307
        sawtooth_values[49] = 0.979221
        sawtooth_values[50] = 0.499567
        sawtooth_values[51] = 0.509957
        sawtooth_values[52] = 0.519481
        sawtooth_values[53] = 0.52987
        sawtooth_values[54] = 0.539394
        sawtooth_values[55] = 0.549784
        sawtooth_values[56] = 0.559307
        sawtooth_values[57] = 0.569697
        sawtooth_values[58] = 0.579221
        sawtooth_values[59] = 0.58961
        sawtooth_values[60] = 0.6
        sawtooth_values[61] = 0.609524
        sawtooth_values[62] = 0.619913
        sawtooth_values[63] = 0.629437
        sawtooth_values[64] = 0.639827
        sawtooth_values[65] = 0.649351
        sawtooth_values[66] = 0.65974
        sawtooth_values[67] = 0.669264
        sawtooth_values[68] = 0.679654
        sawtooth_values[69] = 0.689177
        sawtooth_values[70] = 0.699567
        sawtooth_values[71] = 0.709957
        sawtooth_values[72] = 0.719481
        sawtooth_values[73] = 0.72987
        sawtooth_values[74] = 0.739394
        sawtooth_values[75] = 0.749784
        sawtooth_values[76] = 0.759307
        sawtooth_values[77] = 0.769697
        sawtooth_values[78] = 0.779221
        sawtooth_values[79] = 0.78961
        sawtooth_values[80] = 0.8
        sawtooth_values[81] = 0.809524
        sawtooth_values[82] = 0.819913
        sawtooth_values[83] = 0.829437
        sawtooth_values[84] = 0.839827
        sawtooth_values[85] = 0.849351
        sawtooth_values[86] = 0.85974
        sawtooth_values[87] = 0.869264
        sawtooth_values[88] = 0.879654
        sawtooth_values[89] = 0.889177
        sawtooth_values[90] = 0.899567
        sawtooth_values[91] = 0.909957
        sawtooth_values[92] = 0.919481
        sawtooth_values[93] = 0.92987
        sawtooth_values[94] = 0.939394
        sawtooth_values[95] = 0.949784
        sawtooth_values[96] = 0.959307
        sawtooth_values[97] = 0.969697
        sawtooth_values[98] = 0.979221
        sawtooth_values[99] = 0.98961
        sawtooth_values[100] = 1

        self.sawtooth_values = sawtooth_values

    @property
    def values(self):
        return self.sawtooth_values


class SparkWCPenalty(PercentagePenalty):
    def __init__(self, initial_bump):
        PercentagePenalty.__init__(self, initial_bump)

        sparkwc_values = [0] * 100  # 100 percentages
        sparkwc_values[8] = 1.30336
        sparkwc_values[9] = 1.30336
        sparkwc_values[10] = 1.30336
        sparkwc_values[11] = 1.30336
        sparkwc_values[12] = 1.30336
        sparkwc_values[13] = 1.30336
        sparkwc_values[14] = 1.30336
        sparkwc_values[15] = 1.30336
        sparkwc_values[16] = 1.19489
        sparkwc_values[17] = 1.19489
        sparkwc_values[18] = 1.19489
        sparkwc_values[19] = 1.19489
        sparkwc_values[20] = 1.19489
        sparkwc_values[21] = 1.19489
        sparkwc_values[22] = 1.19489
        sparkwc_values[23] = 1.19489
        sparkwc_values[24] = 1.19489
        sparkwc_values[25] = 1.12963
        sparkwc_values[26] = 1.12963
        sparkwc_values[27] = 1.12963
        sparkwc_values[28] = 1.12963
        sparkwc_values[29] = 1.12963
        sparkwc_values[30] = 1.12963
        sparkwc_values[31] = 1.12963
        sparkwc_values[32] = 1.12963
        sparkwc_values[33] = 1.07407
        sparkwc_values[34] = 1.07407
        sparkwc_values[35] = 1.07407
        sparkwc_values[36] = 1.07407
        sparkwc_values[37] = 1.07407
        sparkwc_values[38] = 1.07407
        sparkwc_values[39] = 1.07407
        sparkwc_values[40] = 1.07407
        sparkwc_values[41] = 1.07231
        sparkwc_values[42] = 1.07231
        sparkwc_values[43] = 1.07231
        sparkwc_values[44] = 1.07231
        sparkwc_values[45] = 1.07231
        sparkwc_values[46] = 1.07231
        sparkwc_values[47] = 1.07231
        sparkwc_values[48] = 1.07231
        sparkwc_values[49] = 1.07231
        sparkwc_values[50] = 0.992946
        sparkwc_values[51] = 0.992946
        sparkwc_values[52] = 0.992946
        sparkwc_values[53] = 0.992946
        sparkwc_values[54] = 0.992946
        sparkwc_values[55] = 0.992946
        sparkwc_values[56] = 0.992946
        sparkwc_values[57] = 0.992946
        sparkwc_values[58] = 0.991184
        sparkwc_values[59] = 0.991184
        sparkwc_values[60] = 0.991184
        sparkwc_values[61] = 0.991184
        sparkwc_values[62] = 0.991184
        sparkwc_values[63] = 0.991184
        sparkwc_values[64] = 0.991184
        sparkwc_values[65] = 0.991184
        sparkwc_values[66] = 1.00133
        sparkwc_values[67] = 1.00133
        sparkwc_values[68] = 1.00133
        sparkwc_values[69] = 1.00133
        sparkwc_values[70] = 1.00133
        sparkwc_values[71] = 1.00133
        sparkwc_values[72] = 1.00133
        sparkwc_values[73] = 1.00133
        sparkwc_values[74] = 1.00133
        sparkwc_values[75] = 0.867726
        sparkwc_values[76] = 0.867726
        sparkwc_values[77] = 0.867726
        sparkwc_values[78] = 0.867726
        sparkwc_values[79] = 0.867726
        sparkwc_values[80] = 0.867726
        sparkwc_values[81] = 0.867726
        sparkwc_values[82] = 0.867726
        sparkwc_values[83] = 0.91226
        sparkwc_values[84] = 0.91226
        sparkwc_values[85] = 0.91226
        sparkwc_values[86] = 0.91226
        sparkwc_values[87] = 0.91226
        sparkwc_values[88] = 0.91226
        sparkwc_values[89] = 0.91226
        sparkwc_values[90] = 0.91226
        sparkwc_values[91] = 1
        sparkwc_values[92] = 1
        sparkwc_values[93] = 1
        sparkwc_values[94] = 1
        sparkwc_values[95] = 1
        sparkwc_values[96] = 1
        sparkwc_values[97] = 1
        sparkwc_values[98] = 1
        sparkwc_values[99] = 1

        self.sparkwc_values = sparkwc_values

    @property
    def values(self):
        return self.sparkwc_values
