name: Deploy to Remote Dev Server

on:
  push:
    branches: [ main, production ]  # Trigger on push to main or production branch
  workflow_dispatch:  # Allow manual triggering

jobs:
  deploy:
    runs-on: ubuntu-latest
    
    steps:
    - name: Checkout code
      uses: actions/checkout@v4
      
    - name: Deploy to server
      uses: appleboy/ssh-action@v1.0.3
      with:
        host: ${{ secrets.DEV_HOST }}
        username: ${{ secrets.DEV_USERNAME }}
        key: ${{ secrets.DEV_SSH_PRIVATE_KEY }}
        port: ${{ secrets.DEV_PORT || 22 }}
        script: |
          # Navigate to your project directory
          cd /home/ubuntu/odoo/custom_addons/icomply_odoo/
          
          # Pull latest changes
          git pull origin main
          # Update compliance_management module
          /home/ubuntu/python3.12-env/bin/python /home/ubuntu/odoo/odoo-bin -c /home/ubuntu/odoo/debian/odoo.conf -d compliance_dev -i compliance_management --stop-after-init  
          echo "Deployment completed successfully!"