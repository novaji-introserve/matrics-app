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
          
          # Install/update dependencies (uncomment as needed)
          # npm install
          # composer install --no-dev --optimize-autoloader
          # pip install -r requirements.txt
          
          # Build assets (uncomment as needed)
          # npm run build
          # npm run production
          
          # Set proper permissions
          # chmod -R 755 storage bootstrap/cache
          
          # Restart services (uncomment as needed)
          # sudo systemctl restart nginx
          # sudo systemctl restart php8.1-fpm
          # pm2 restart all
          # sudo supervisorctl restart all
          
          echo "Deployment completed successfully!"