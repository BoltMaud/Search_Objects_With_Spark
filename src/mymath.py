import math

'''
get all the prime factors of n
@:return list of int 
'''
def get_prime_factors(n):
    factors= []
    if n==1:
        return factors
    while n>=2:
        x,r = divmod(n,2)
        if r!=0:
            break
        factors.append(2)
        n = x
    i=3
    rn = math.sqrt(n)+1
    while i<=n:
        if i>rn:
            factors.append(n)
            break
        x,r = divmod(n,i)
        if r==0:
            factors.append(i)
            n=x
            rn = math.sqrt(n)+1
        else:
            i += 2
    return factors


'''
@:return True if prime else False
'''
def is_prime(n):
    for i in range(2, int(n**0.5)+1):
        if n%i==0:
            return False
    return True


'''
transform ra from celeste to equatorial
'''
def getL(ra, decl):
    e = 23.439281
    l = math.atan( ((math.sin(e)\
                     /math.cos(ra))\
                    * math.tan(decl))\
                   +( (math.cos(e)/\
                       math.cos(ra))\
                      *math.tan(ra)))
    return l

def getB(ra, decl):
    e = 23.439281
    b= math.asin( ( math.cos(e) * math.sin(decl)) - (math.sin(e)*math.sin(ra)*math.cos(decl))  )
    return b