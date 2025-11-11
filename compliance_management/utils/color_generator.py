
class ColorGenerator:
    """Service for generating chart colors with improved performance.
    
    This class provides functionalities to generate colors based on predefined
    color schemes, optimizing for performance by reusing color values when necessary.
    """
    
    COLOR_SCHEMES = {
        "cool": [
            "#0077B6", "#30454D", "#030405", "#678187", "#80FFDB",
            "#277A70", "#6930C3", "#5E60CE", "#5390D9", "#4361EE"
        ],
        "warm": [
            "#FF6B00", "#C7AC9B", "#2C5C3F", "#938A5C",
            "#29180E", "#513535", "#654F57", "#FF4D6D",
            "#C1121F", "#FAA307"
        ],
        "rainbow": [
            "#FF0000", "#FF7F00", "#FFFF00", "#7FFF00",
            "#00FF00", "#24342C", "#00FFFF", "#3F5871",
            "#0000FF", "#8B00FF"
        ],
        "brown": [
            "#5A3E36", "#7A5C2E", "#A97142", "#C2B280", "#B38B6D",
            "#484744", "#C26448", "#DCA888", "#120D08", "#6B705C"
        ],
        "default": [
            "#6B705C", "#B7B7A4", "#A68A64", "#CB997E", "#DDB892",
            "#EDE0D4", "#B08968", "#7F5539", "#9C6644", "#5E503F"
        ]
    }

    
    def _generate_colors(self, color_scheme, count):
        """Generate colors based on the selected color scheme.

        Args:
            color_scheme (str): The name of the color scheme to use.
            count (int): The number of colors to generate.

        Returns:
            list: A list of color codes generated based on the specified scheme.

        The method optimizes performance by using modulo for cycling through 
        the color scheme if the requested count exceeds the available colors.
        """
        base_colors = self.COLOR_SCHEMES.get(color_scheme, self.COLOR_SCHEMES['default'])
        
        if count <= len(base_colors):
            return base_colors[:count]
        
        return [base_colors[i % len(base_colors)] for i in range(count)]