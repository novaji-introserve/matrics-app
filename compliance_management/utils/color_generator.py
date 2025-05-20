class ColorGenerator:
    """Service for generating chart colors with improved performance"""
    
    # Color schemes as class attributes for better performance
    COLOR_SCHEMES = {
        'cool': ['#3366cc', '#66ccff', '#6666ff', '#3333cc', '#000099'],
        'warm': ['#ff6600', '#ff9933', '#ffcc66', '#ff0000', '#cc0000'],
        'rainbow': ['#ff0000', '#ff9900', '#ffff00', '#00ff00', '#0099ff', '#6633ff'],
        'brown': [
            '#483E1D', '#F2D473', '#564B2B', '#ECDFA4', '#83733F',
            '#ECE1A2', '#5F5330', '#B78C00', '#6A5D36', '#C4AA55'
        ],
        'default': [
            '#483E1D', '#F2D473', '#564B2B', '#ECDFA4', '#83733F',
            '#ECE1A2', '#5F5330', '#B78C00', '#6A5D36', '#C4AA55'
        ]
    }
    
    def _generate_colors(self, color_scheme, count):
        """Generate colors based on the selected color scheme with optimized performance"""
        # Get the color scheme or use default if not found
        base_colors = self.COLOR_SCHEMES.get(color_scheme, self.COLOR_SCHEMES['default'])
        
        # For small counts, return immediately without list comprehension
        if count <= len(base_colors):
            return base_colors[:count]
        
        # For larger counts, use list comprehension but with modulo for cycling
        return [base_colors[i % len(base_colors)] for i in range(count)]




