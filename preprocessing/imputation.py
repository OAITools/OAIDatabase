import numpy as np

# deal with missing values

def ses( v, idx, alpha=0.1 ) :
    '''Simple Exponential Smoothing: Recursively smooth series data
    
    '''
    if idx == 0:
        return
    ses(v, idx-1, alpha );
    v[idx] = ( alpha *  v[idx] ) + ( 1.0 - alpha ) * v[idx-1];


def interpolate(v):
    '''Interpolate missing values in column. Compute the mean 
    of the nearest pre and post observation values. 
    NOTE: We could also use the mean of all obs,  exponential
    smoothing, etc. here if we like. 
    
    '''
    for j in range(0,v.shape[1]):
        
        row = v[...,j]    
        for i in range(1,len(row)-1):
            if not np.isnan(row[i]):
                continue
            a = row[i-1]
            b = row[i+1]
            k = i + 1
            
            while np.isnan(b) and k < len(row)-1:
                k += 1
                b = row[k]
            row[i] = (a+b)/2.0
        v[...,j] = row