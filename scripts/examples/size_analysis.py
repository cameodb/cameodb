#!/usr/bin/env python3
"""
CameoDB Batch Ingestion Size Analysis

Analyzes memory usage, system limits, and performance implications
for different batch sizes across all ingestion scripts.
"""

def analyze_all_datasets():
    """Comprehensive analysis of all dataset configurations."""
    
    # Current configurations
    configs = {
        'books': {
            'batch_size': 1000,
            'memory_limit_mb': 8,
            'doc_size_bytes': 6180,
            'total_records': 16559,
            'old_batch_size': 200,
        },
        'ted': {
            'batch_size': 1000,
            'memory_limit_mb': 4,
            'doc_size_bytes': 2570,
            'total_records': 4650,
            'old_batch_size': 400,
        },
        'urls': {
            'batch_size': 1000,
            'memory_limit_mb': 2,
            'doc_size_bytes': 146,
            'total_records': 39,
            'old_batch_size': 200,
        }
    }
    
    # Actor message overhead estimation
    client_op_overhead = 200  # Base ClientOp enum overhead
    vec_overhead = 24  # Vec header per element
    doc_payload_overhead = 100  # DocPayload struct overhead
    batch_request_overhead = 150  # BatchWriteRequest struct
    
    print('🔍 CameoDB Batch Ingestion Size Analysis')
    print('=' * 50)
    print()
    
    # System limits
    print('📊 System Limits')
    print(f'HTTP Body Limit: 200MB')
    print(f'Kameo Remote Request: 64MB')
    print(f'Kameo Remote Response: 64MB')
    print()
    
    # Analyze each dataset
    for name, config in configs.items():
        print(f'📚 {name.title()} Dataset Analysis')
        print('-' * 30)
        
        # Calculate sizes
        doc_total_size = config['doc_size_bytes'] + doc_payload_overhead + vec_overhead
        batch_total = (doc_total_size * config['batch_size']) + (client_op_overhead + batch_request_overhead)
        batch_total_mb = batch_total / 1024 / 1024
        
        # Batch calculations
        old_batches = (config['total_records'] + config['old_batch_size'] - 1) // config['old_batch_size']
        new_batches = (config['total_records'] + config['batch_size'] - 1) // config['batch_size']
        improvement = (old_batches - new_batches) / old_batches * 100
        
        # Safety calculations
        kameo_usage_percent = batch_total_mb / 64 * 100
        peak_memory_mb = batch_total_mb * 3  # 3x multiplication during processing
        memory_margin_mb = config['memory_limit_mb'] - batch_total_mb
        
        print(f'Configuration:')
        print(f'  Records: {config["total_records"]:,}')
        print(f'  Batch Size: {config["batch_size"]} docs')
        print(f'  Memory Limit: {config["memory_limit_mb"]}MB')
        print(f'  Actual Usage: {batch_total_mb:.2f}MB')
        print(f'  Memory Margin: {memory_margin_mb:.2f}MB')
        print(f'  Peak Memory: {peak_memory_mb:.2f}MB')
        print()
        
        print(f'Performance Impact:')
        print(f'  Batches: {old_batches} → {new_batches} (-{improvement:.1f}%)')
        print(f'  HTTP Requests: {improvement:.1f}% fewer')
        print(f'  Expected Throughput: ~{int(config["total_records"] / (new_batches * 0.8)):,} docs/sec')
        print()
        
        print(f'Safety Analysis:')
        print(f'  Kameo Usage: {kameo_usage_percent:.1f}%')
        print(f'  Safety Factor: {64 / batch_total_mb:.1f}x headroom')
        
        # Safety rating
        if kameo_usage_percent < 20:
            status = '✅ VERY SAFE'
        elif kameo_usage_percent < 50:
            status = '✅ SAFE'
        elif kameo_usage_percent < 80:
            status = '⚠️  CAUTION'
        else:
            status = '❌ UNSAFE'
        
        print(f'  Status: {status}')
        print()
    
    # Summary
    print('🎯 Summary & Recommendations')
    print('-' * 30)
    print('✅ All configurations under 10% of Kameo limits')
    print('✅ Peak memory usage under 20MB for all datasets')
    print('✅ Significant performance improvements achieved')
    print('✅ Plenty of headroom for future scaling')
    print()
    print('📈 Performance Gains:')
    print('  Books: 83 → 17 batches (-79.5%)')
    print('  TED: 12 → 5 batches (-58.3%)')
    print('  URLs: 1 → 1 batches (optimal)')
    print()
    print('🛡️ Safety Margins:')
    print('  Books: 58MB margin under Kameo limit')
    print('  TED: 61MB margin under Kameo limit')
    print('  URLs: 64MB margin under Kameo limit')

def analyze_custom_batch_size(batch_size: int, dataset: str = 'books'):
    """Analyze custom batch size for a specific dataset."""
    
    doc_sizes = {
        'books': 6180,
        'ted': 2570,
        'urls': 146
    }
    
    memory_limits = {
        'books': 8,
        'ted': 4,
        'urls': 2
    }
    
    if dataset not in doc_sizes:
        print(f'❌ Unknown dataset: {dataset}')
        print(f'Available datasets: {list(doc_sizes.keys())}')
        return
    
    # Overhead calculations
    doc_size = doc_sizes[dataset]
    memory_limit = memory_limits[dataset]
    client_op_overhead = 200
    vec_overhead = 24
    doc_payload_overhead = 100
    batch_request_overhead = 150
    
    doc_total_size = doc_size + doc_payload_overhead + vec_overhead
    batch_total = (doc_total_size * batch_size) + (client_op_overhead + batch_request_overhead)
    batch_total_mb = batch_total / 1024 / 1024
    
    print(f'🔧 Custom Batch Size Analysis: {dataset.title()}')
    print('-' * 40)
    print(f'Batch Size: {batch_size} docs')
    print(f'Memory Usage: {batch_total_mb:.2f}MB')
    print(f'Memory Limit: {memory_limit}MB')
    
    if batch_total_mb <= memory_limit:
        print(f'✅ WITHIN LIMIT: {memory_limit - batch_total_mb:.2f}MB margin')
    else:
        print(f'❌ EXCEEDS LIMIT: {batch_total_mb - memory_limit:.2f}MB over')
    
    kameo_usage = batch_total_mb / 64 * 100
    print(f'Kameo Usage: {kameo_usage:.1f}%')
    
    if kameo_usage < 20:
        print('✅ VERY SAFE: Under 20% of Kameo limit')
    elif kameo_usage < 50:
        print('✅ SAFE: Under 50% of Kameo limit')
    elif kameo_usage < 80:
        print('⚠️  CAUTION: Approaching 80% of Kameo limit')
    else:
        print('❌ UNSAFE: Over 80% of Kameo limit')

if __name__ == '__main__':
    import sys
    
    if len(sys.argv) > 1:
        if len(sys.argv) == 3:
            try:
                batch_size = int(sys.argv[1])
                dataset = sys.argv[2].lower()
                analyze_custom_batch_size(batch_size, dataset)
            except ValueError:
                print('❌ Invalid batch size. Usage: python size_analysis.py [batch_size] [dataset]')
        else:
            print('❌ Invalid arguments. Usage: python size_analysis.py [batch_size] [dataset]')
    else:
        analyze_all_datasets()
