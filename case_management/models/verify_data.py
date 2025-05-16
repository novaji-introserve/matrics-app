from .exception import ExceptionProcess
from .exception import ExceptionProcessType


def verify_exception_data(env):
    """Verify and report on exception process data"""
    
    # Check process types
    types = env['exception.process.type'].search([])
    print(f"\n=== Exception Process Types: {len(types)} records ===")
    if not types:
        print("WARNING: No process types found - dropdowns will be empty!")
    else:
        for t in types[:5]:  # Show first 5 records
            print(f"ID: {t.id}, Name: {t.name}, NUM_ID: {t.num_id}")
        if len(types) > 5:
            print(f"... and {len(types) - 5} more")
    
    # Check processes
    processes = env['exception.process'].search([])
    print(f"\n=== Exception Processes: {len(processes)} records ===")
    if not processes:
        print("WARNING: No processes found - dropdowns will be empty!")
    else:
        for p in processes[:5]:  # Show first 5 records
            type_name = p.type_id.name if p.type_id else "None"
            print(f"ID: {p.id}, Name: {p.name}, Type: {type_name}")
        if len(processes) > 5:
            print(f"... and {len(processes) - 5} more")
    
    # Check relationship between process and type
    no_type = env['exception.process'].search([('type_id', '=', False)])
    if no_type:
        print(f"\nWARNING: {len(no_type)} processes have no type assigned!")
    
    # Check for duplicate NUM_ID in types
    duplicates = env.cr.execute("""
        SELECT num_id, COUNT(*) 
        FROM exception_process_type 
        GROUP BY num_id 
        HAVING COUNT(*) > 1
    """)
    duplicates = env.cr.fetchall()
    if duplicates:
        print("\nWARNING: Found duplicate NUM_IDs in process types:")
        for num_id, count in duplicates:
            print(f"NUM_ID {num_id}: {count} occurrences")

# Sample data for testing
def create_test_data(env):
    """Create sample data for testing the dropdowns"""
    
    # Create process types if none exist
    if not env['exception.process.type'].search([]):
        print("\nCreating sample process types...")
        types = [
            {'name': 'Regulatory', 'num_id': 1, 'active': True},
            {'name': 'Operational', 'num_id': 2, 'active': True},
            {'name': 'Compliance', 'num_id': 3, 'active': True}
        ]
        for t in types:
            env['exception.process.type'].create(t)
    
    # Create processes if none exist
    if not env['exception.process'].search([]):
        print("Creating sample processes...")
        type_records = env['exception.process.type'].search([])
        if type_records:
            processes = [
                {'name': 'AML Monitoring', 'type_id': type_records[0].id},
                {'name': 'KYC Verification', 'type_id': type_records[0].id},
                {'name': 'Risk Assessment', 'type_id': type_records[1].id},
                {'name': 'Audit Review', 'type_id': type_records[2].id}
            ]
            for p in processes:
                env['exception.process'].create(p)
    
    print("Sample data creation complete.")