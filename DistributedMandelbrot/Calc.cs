using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DistributedMandelbrot
{
    public static class Calc
    {

        public static double Lerp(double a, double b, double t)
            => ((b - a) * t) + a;

    }
}
